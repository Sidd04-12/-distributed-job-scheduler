package com.sidd.scheduler;

import com.sidd.scheduler.domain.Job;
import com.sidd.scheduler.domain.JobStatus;
import com.sidd.scheduler.repo.JobRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kafka delivers at-least-once, so the same job will occasionally reach a worker twice. What
 * makes execution effectively-once is that only one caller can move the row out of DISPATCHED.
 * These tests hammer that transition concurrently and assert exactly one winner.
 * <p>
 * Every contending thread runs inside its own transaction, which is what separate worker
 * processes actually do — running them in a shared transaction would test nothing, because the
 * conflict this guards against is between transactions.
 */
class IdempotencyGuardTest extends IntegrationTestBase {

    @Autowired
    JobRepository jobs;

    @Autowired
    TransactionTemplate tx;

    /** Tests share a database with the running stack, so they clean up after themselves. */
    private final List<UUID> created = new CopyOnWriteArrayList<>();

    @AfterEach
    void removeTestRows() {
        tx.executeWithoutResult(s -> jobs.deleteAllById(created));
        created.clear();
    }

    private Job persistedJob(JobStatus status) {
        Job job = tx.execute(s -> {
            Job j = new Job();
            j.setId(UUID.randomUUID());
            j.setType("echo");
            j.setPayload("{}");
            j.setPriority(5);
            j.setStatus(status);
            j.setRunAt(Instant.now());
            return jobs.saveAndFlush(j);
        });
        created.add(job.getId());
        return job;
    }

    private int claim(UUID id, String workerId) {
        return tx.execute(s -> jobs.transitionIfStatus(
                id, JobStatus.DISPATCHED, JobStatus.RUNNING, workerId, Instant.now()));
    }

    private int cancel(UUID id) {
        return tx.execute(s -> jobs.transitionIfStatusIn(
                id, List.of(JobStatus.SCHEDULED, JobStatus.DISPATCHED),
                JobStatus.CANCELLED, Instant.now()));
    }

    @Test
    void onlyOneOfManyConcurrentClaimsWins() throws Exception {
        int contenders = 16;
        Job job = persistedJob(JobStatus.DISPATCHED);

        ExecutorService pool = Executors.newFixedThreadPool(contenders);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger winners = new AtomicInteger();

        List<Future<Integer>> results = new ArrayList<>();
        for (int i = 0; i < contenders; i++) {
            String workerId = "worker-" + i;
            results.add(pool.submit(() -> {
                start.await();
                int updated = claim(job.getId(), workerId);
                if (updated > 0) {
                    winners.incrementAndGet();
                }
                return updated;
            }));
        }

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        for (Future<Integer> r : results) {
            r.get();
        }

        assertEquals(1, winners.get(), "more than one worker claimed the same job");

        Job after = jobs.findById(job.getId()).orElseThrow();
        assertEquals(JobStatus.RUNNING, after.getStatus());
        assertEquals(1, after.getAttempts(), "attempt counter incremented more than once");
    }

    @Test
    void duplicateDeliveryAfterCompletionDoesNotReRun() {
        Job job = persistedJob(JobStatus.COMPLETED);

        assertEquals(0, claim(job.getId(), "late-worker"), "a completed job must not be re-claimed");
        assertEquals(JobStatus.COMPLETED, jobs.findById(job.getId()).orElseThrow().getStatus());
    }

    @Test
    void cancelledJobIsNeverClaimedByAWorker() {
        Job job = persistedJob(JobStatus.DISPATCHED);

        // Cancel wins the row first...
        assertEquals(1, cancel(job.getId()));

        // ...so the in-flight dispatch finds nothing to claim.
        assertEquals(0, claim(job.getId(), "worker-1"), "a cancelled job was picked up by a worker");
        assertEquals(JobStatus.CANCELLED, jobs.findById(job.getId()).orElseThrow().getStatus());
    }

    @Test
    void concurrentCancelAndClaimProduceExactlyOneOutcome() throws Exception {
        int rounds = 150;
        int cancelWins = 0;
        int claimWins = 0;
        ExecutorService pool = Executors.newFixedThreadPool(4);

        try {
            for (int i = 0; i < rounds; i++) {
                Job job = persistedJob(JobStatus.DISPATCHED);
                CountDownLatch start = new CountDownLatch(1);

                Future<Integer> cancelResult = pool.submit(() -> {
                    start.await();
                    return cancel(job.getId());
                });
                Future<Integer> claimResult = pool.submit(() -> {
                    start.await();
                    return claim(job.getId(), "worker-1");
                });

                start.countDown();
                int c = cancelResult.get(20, TimeUnit.SECONDS);
                int r = claimResult.get(20, TimeUnit.SECONDS);

                // Whichever way they interleave, the row ends in exactly one state and the other
                // caller is told it lost. What must never happen is both succeeding.
                assertEquals(1, c + r, "cancel and claim both succeeded on the same job");
                if (c == 1) cancelWins++; else claimWins++;
            }
        } finally {
            pool.shutdownNow();
        }

        assertEquals(rounds, cancelWins + claimWins);
    }
}
