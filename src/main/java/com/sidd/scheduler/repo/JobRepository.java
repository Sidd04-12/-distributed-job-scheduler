package com.sidd.scheduler.repo;

import com.sidd.scheduler.domain.Job;
import com.sidd.scheduler.domain.JobStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {

    Page<Job> findByStatus(JobStatus status, Pageable pageable);

    /**
     * The idempotency guard.
     * <p>
     * Kafka delivers at least once, so a worker will occasionally receive the same job twice —
     * after a rebalance, or when an offset commit is lost. This update only matches rows still
     * in {@code expected} status, so the first delivery transitions the row and the second
     * matches nothing and returns 0. The caller treats 0 as "someone else has this, skip it".
     * <p>
     * The database's row lock does the mutual exclusion, which is why this holds across any
     * number of worker processes on any number of machines. It is also what makes a cancelled
     * job safe: CANCELLED is not {@code expected}, so the update fails and the worker never runs
     * it, no matter how the cancel and the dispatch interleave.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE Job j
               SET j.status = :next,
                   j.workerId = :workerId,
                   j.attempts = j.attempts + 1,
                   j.updatedAt = :now
             WHERE j.id = :id
               AND j.status = :expected
            """)
    int transitionIfStatus(@Param("id") UUID id,
                           @Param("expected") JobStatus expected,
                           @Param("next") JobStatus next,
                           @Param("workerId") String workerId,
                           @Param("now") Instant now);

    /** Same conditional-transition trick, without touching the attempt counter. */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("""
            UPDATE Job j
               SET j.status = :next,
                   j.updatedAt = :now
             WHERE j.id = :id
               AND j.status IN :expected
            """)
    int transitionIfStatusIn(@Param("id") UUID id,
                             @Param("expected") java.util.Collection<JobStatus> expected,
                             @Param("next") JobStatus next,
                             @Param("now") Instant now);

    long countByStatus(JobStatus status);
}
