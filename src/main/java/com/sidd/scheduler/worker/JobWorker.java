package com.sidd.scheduler.worker;

import com.sidd.scheduler.api.dto.JobDtos.JobDispatchedEvent;
import com.sidd.scheduler.domain.Job;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Consumes dispatched jobs and runs them.
 * <p>
 * Every instance joins the same Kafka consumer group, so adding worker replicas adds throughput
 * with no code change — Kafka rebalances partitions across whoever is alive. Delivery is
 * at-least-once, so the same job can arrive twice; {@link JobClaimer} is what makes execution
 * effectively-once.
 * <p>
 * Note what this method does <em>not</em> do: hold a transaction. The claim and the outcome are
 * each their own short transaction, and the actual work happens between them with no database
 * connection held.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class JobWorker {

    private final JobClaimer claimer;
    private final JobExecutor executor;
    private final JobOutcomeHandler outcome;

    @Value("${scheduler.worker.id:worker-1}")
    private String workerId;

    @KafkaListener(
            topics = "${scheduler.topics.ready}",
            groupId = "${scheduler.worker.group-id:job-workers}",
            concurrency = "${scheduler.worker.concurrency:3}")
    public void onJobDispatched(JobDispatchedEvent event) {
        Optional<Job> claimed = claimer.claim(event.jobId(), workerId);

        if (claimed.isEmpty()) {
            // Lost the race to another worker, or the job was cancelled or already finished.
            // A duplicate Kafka delivery lands here and is correctly ignored.
            log.info("Job {} not claimable by {} — skipping", event.jobId(), workerId);
            return;
        }

        Job job = claimed.get();
        try {
            outcome.complete(job, executor.execute(job), workerId);
        } catch (Exception e) {
            outcome.fail(job, e, workerId);
        }
    }
}
