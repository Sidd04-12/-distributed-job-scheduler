package com.sidd.scheduler.api;

import com.sidd.scheduler.api.dto.JobDtos.SubmitJobRequest;
import com.sidd.scheduler.domain.Job;
import com.sidd.scheduler.domain.JobEvent;
import com.sidd.scheduler.domain.JobStatus;
import com.sidd.scheduler.queue.JobQueue;
import com.sidd.scheduler.repo.JobEventRepository;
import com.sidd.scheduler.repo.JobRepository;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class JobService {

    private final JobRepository jobs;
    private final JobEventRepository events;
    private final JobQueue queue;

    @Transactional
    public Job submit(SubmitJobRequest req) {
        Instant runAt = resolveRunAt(req);

        Job job = new Job();
        job.setId(UUID.randomUUID());
        job.setType(req.type());
        job.setPayload(req.payload() == null ? "{}" : req.payload());
        job.setPriority(req.priority() == null ? 5 : req.priority());
        job.setMaxAttempts(req.maxAttempts() == null ? 5 : req.maxAttempts());
        job.setStatus(JobStatus.SCHEDULED);
        job.setRunAt(runAt);
        jobs.save(job);
        events.save(JobEvent.of(job.getId(), null, JobStatus.SCHEDULED, "submitted", null));

        // Postgres is written first so a crash here leaves a recoverable row rather than a
        // queue entry pointing at a job that does not exist.
        queue.enqueue(job.getId(), runAt);
        log.info("Submitted job {} type={} priority={} runAt={}",
                job.getId(), job.getType(), job.getPriority(), runAt);
        return job;
    }

    private Instant resolveRunAt(SubmitJobRequest req) {
        if (req.runAt() != null) {
            return req.runAt();
        }
        if (req.delaySeconds() != null && req.delaySeconds() > 0) {
            return Instant.now().plusSeconds(req.delaySeconds());
        }
        return Instant.now();
    }

    /**
     * Cancel is best-effort by nature: the job may already be executing. Removing it from Redis
     * stops it being dispatched, and the status write means that if it was dispatched a moment
     * ago, the worker's conditional transition will fail and it will never run.
     */
    @Transactional
    public boolean cancel(UUID jobId) {
        Job job = jobs.findById(jobId)
                .orElseThrow(() -> new EntityNotFoundException("No job " + jobId));

        queue.remove(jobId);
        int updated = jobs.transitionIfStatusIn(
                jobId, List.of(JobStatus.SCHEDULED, JobStatus.DISPATCHED, JobStatus.FAILED),
                JobStatus.CANCELLED, Instant.now());

        if (updated > 0) {
            events.save(JobEvent.of(jobId, job.getStatus(), JobStatus.CANCELLED, "cancelled by request", null));
            log.info("Cancelled job {}", jobId);
            return true;
        }
        log.info("Cancel rejected for job {} — status was {}", jobId, job.getStatus());
        return false;
    }

    @Transactional(readOnly = true)
    public Job get(UUID jobId) {
        return jobs.findById(jobId).orElseThrow(() -> new EntityNotFoundException("No job " + jobId));
    }

    @Transactional(readOnly = true)
    public Page<Job> list(JobStatus status, Pageable pageable) {
        return status == null ? jobs.findAll(pageable) : jobs.findByStatus(status, pageable);
    }

    @Transactional(readOnly = true)
    public List<JobEvent> history(UUID jobId) {
        return events.findByJobIdOrderByAtAsc(jobId);
    }
}
