package com.sidd.scheduler.worker;

import com.sidd.scheduler.domain.Job;
import com.sidd.scheduler.domain.JobEvent;
import com.sidd.scheduler.domain.JobStatus;
import com.sidd.scheduler.repo.JobEventRepository;
import com.sidd.scheduler.repo.JobRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Wins (or loses) the right to execute a job.
 * <p>
 * Deliberately its own short transaction, separate from executing the job. Running the whole
 * listener in one transaction would hold a database connection for the entire duration of the
 * work — fine for a 5ms job, ruinous for a 30-second one, and a good way to exhaust the pool
 * under load. So the sequence is: claim (short transaction) → execute (no transaction) →
 * record the outcome (short transaction).
 */
@Component
@RequiredArgsConstructor
public class JobClaimer {

    private final JobRepository jobs;
    private final JobEventRepository events;

    /**
     * @return the job if this caller won the claim, empty if another worker already has it,
     *         it was cancelled, or it already finished
     */
    @Transactional
    public Optional<Job> claim(UUID jobId, String workerId) {
        int won = jobs.transitionIfStatus(
                jobId, JobStatus.DISPATCHED, JobStatus.RUNNING, workerId, Instant.now());

        if (won == 0) {
            return Optional.empty();
        }

        events.save(JobEvent.of(jobId, JobStatus.DISPATCHED, JobStatus.RUNNING, "claimed", workerId));
        return jobs.findById(jobId);
    }
}
