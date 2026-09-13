package com.sidd.scheduler.dispatch;

import com.sidd.scheduler.api.dto.JobDtos.JobDispatchedEvent;
import com.sidd.scheduler.config.SchedulerProperties;
import com.sidd.scheduler.domain.Job;
import com.sidd.scheduler.domain.JobEvent;
import com.sidd.scheduler.domain.JobStatus;
import com.sidd.scheduler.repo.JobEventRepository;
import com.sidd.scheduler.repo.JobRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Holds the transactional half of dispatch.
 * <p>
 * This lives in its own bean deliberately. Spring applies {@code @Transactional} through a proxy,
 * so a method called from another method of the same class bypasses the proxy entirely and runs
 * with no transaction — silently. Keeping the transactional unit in a separate bean means the
 * proxy is actually in the call path.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DispatchExecutor {

    private final JobRepository jobs;
    private final JobEventRepository events;
    private final KafkaTemplate<String, Object> kafka;
    private final SchedulerProperties props;

    /** @return true if the job was dispatched, false if it was no longer eligible */
    @Transactional
    public boolean dispatch(Job job) {
        int updated = jobs.transitionIfStatus(
                job.getId(), JobStatus.SCHEDULED, JobStatus.DISPATCHED, null, Instant.now());

        if (updated == 0) {
            // Cancelled between the Redis claim and here, or already handled by someone else.
            log.debug("Skipping job {} — no longer SCHEDULED", job.getId());
            return false;
        }

        events.save(JobEvent.of(job.getId(), JobStatus.SCHEDULED, JobStatus.DISPATCHED, "dispatched", null));

        JobDispatchedEvent event = new JobDispatchedEvent(
                job.getId(), job.getType(), job.getPriority(), job.getAttempts(), Instant.now());

        // Keying by job id pins every delivery for a job to one partition, so its retries stay
        // ordered relative to one another.
        kafka.send(props.getTopics().getReady(), job.getId().toString(), event);
        log.info("Dispatched job {} type={} priority={}", job.getId(), job.getType(), job.getPriority());
        return true;
    }
}
