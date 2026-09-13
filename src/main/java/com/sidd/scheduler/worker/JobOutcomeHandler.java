package com.sidd.scheduler.worker;

import com.sidd.scheduler.api.dto.JobDtos.JobDispatchedEvent;
import com.sidd.scheduler.config.SchedulerProperties;
import com.sidd.scheduler.domain.Job;
import com.sidd.scheduler.domain.JobEvent;
import com.sidd.scheduler.domain.JobStatus;
import com.sidd.scheduler.queue.JobQueue;
import com.sidd.scheduler.repo.JobEventRepository;
import com.sidd.scheduler.repo.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Terminal handling for a job attempt — success, retry, or dead-letter.
 * <p>
 * Separate bean for the same reason as {@code DispatchExecutor}: {@code @Transactional} is applied
 * by a proxy, and a self-invoked method silently runs without a transaction.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class JobOutcomeHandler {

    private final JobRepository jobs;
    private final JobEventRepository events;
    private final JobQueue queue;
    private final KafkaTemplate<String, Object> kafka;
    private final SchedulerProperties props;

    @Transactional
    public void complete(Job job, String result, String workerId) {
        job.setStatus(JobStatus.COMPLETED);
        job.setResult(result);
        job.setCompletedAt(Instant.now());
        job.setUpdatedAt(Instant.now());
        job.setLastError(null);
        jobs.save(job);
        events.save(JobEvent.of(job.getId(), JobStatus.RUNNING, JobStatus.COMPLETED, result, workerId));
        log.info("Job {} completed by {}", job.getId(), workerId);
    }

    /**
     * Retry with exponential backoff and jitter until attempts are exhausted, then dead-letter.
     * The jitter is not decoration: without it, a batch of jobs that failed together retries in
     * lockstep and stampedes whatever they were failing against.
     */
    @Transactional
    public void fail(Job job, Exception error, String workerId) {
        String message = error.getClass().getSimpleName() + ": " + error.getMessage();

        if (job.getAttempts() >= job.getMaxAttempts()) {
            job.setStatus(JobStatus.DEAD_LETTER);
            job.setLastError(message);
            job.setUpdatedAt(Instant.now());
            jobs.save(job);
            events.save(JobEvent.of(job.getId(), JobStatus.RUNNING, JobStatus.DEAD_LETTER, message, workerId));
            kafka.send(props.getTopics().getDlq(), job.getId().toString(),
                    new JobDispatchedEvent(job.getId(), job.getType(), job.getPriority(),
                            job.getAttempts(), Instant.now()));
            log.warn("Job {} exhausted {} attempts — dead-lettered", job.getId(), job.getMaxAttempts());
            return;
        }

        Duration backoff = backoffFor(job.getAttempts());
        Instant nextRun = Instant.now().plus(backoff);

        job.setStatus(JobStatus.SCHEDULED);
        job.setLastError(message);
        job.setRunAt(nextRun);
        job.setUpdatedAt(Instant.now());
        jobs.save(job);
        events.save(JobEvent.of(job.getId(), JobStatus.RUNNING, JobStatus.SCHEDULED,
                "retry in " + backoff.toMillis() + "ms after: " + message, workerId));

        queue.enqueue(job.getId(), nextRun);
        log.info("Job {} failed (attempt {}/{}) — retrying in {}ms",
                job.getId(), job.getAttempts(), job.getMaxAttempts(), backoff.toMillis());
    }

    /** base * 2^(attempt-1), capped at max, plus up to 20% jitter. */
    public Duration backoffFor(int attempt) {
        long base = props.getRetry().getBaseBackoffMs();
        long max = props.getRetry().getMaxBackoffMs();
        long exp = base * (1L << Math.min(Math.max(attempt - 1, 0), 16));
        long capped = Math.min(exp, max);
        long jitter = ThreadLocalRandom.current().nextLong(0, Math.max(1, capped / 5));
        return Duration.ofMillis(capped + jitter);
    }
}
