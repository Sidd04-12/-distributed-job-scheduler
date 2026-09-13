package com.sidd.scheduler.dispatch;

import com.sidd.scheduler.config.SchedulerProperties;
import com.sidd.scheduler.domain.Job;
import com.sidd.scheduler.queue.JobQueue;
import com.sidd.scheduler.repo.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/**
 * Moves due jobs from the Redis queue onto Kafka.
 * <p>
 * Safe to run on every replica: {@link JobQueue#claimDue} is atomic, so a given job is handed to
 * exactly one dispatcher. Priority is applied here, across the batch of jobs that are already
 * due — which keeps scheduled delays exact while still letting a priority-1 job overtake a
 * priority-9 one that came due in the same tick.
 */
@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "scheduler.dispatcher.enabled", havingValue = "true", matchIfMissing = true)
public class Dispatcher {

    private final JobQueue queue;
    private final JobRepository jobs;
    private final DispatchExecutor executor;
    private final SchedulerProperties props;

    @Scheduled(fixedDelayString = "${scheduler.dispatcher.poll-interval-ms:500}")
    public void pollAndDispatch() {
        List<UUID> claimed = queue.claimDue(Instant.now(), props.getDispatcher().getBatchSize());
        if (claimed.isEmpty()) {
            return;
        }

        List<Job> batch = jobs.findAllById(claimed);
        batch.sort(Comparator.comparingInt(Job::getPriority).thenComparing(Job::getRunAt));

        for (Job job : batch) {
            try {
                executor.dispatch(job);
            } catch (Exception e) {
                // The claim already removed it from Redis, so put it back rather than lose it.
                log.error("Dispatch failed for job {} — requeueing", job.getId(), e);
                queue.enqueue(job.getId(), Instant.now().plusSeconds(5));
            }
        }
    }
}
