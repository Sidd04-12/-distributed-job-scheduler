package com.sidd.scheduler.worker;

import com.sidd.scheduler.domain.Job;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Stands in for real work. Job types are deliberately simple and predictable so the scheduler's
 * behaviour — retries, backoff, dead-lettering — can be exercised deterministically in tests.
 *
 * <ul>
 *   <li>{@code echo}        — succeeds immediately, returns its payload</li>
 *   <li>{@code sleep}       — sleeps briefly, to simulate slow work</li>
 *   <li>{@code always-fail} — always throws, to drive the retry path into the dead-letter topic</li>
 *   <li>{@code flaky}       — fails roughly half the time, to exercise retry-then-succeed</li>
 * </ul>
 */
@Component
public class JobExecutor {

    public String execute(Job job) throws Exception {
        switch (job.getType()) {
            case "sleep" -> {
                Thread.sleep(250);
                return "slept 250ms";
            }
            case "always-fail" -> throw new IllegalStateException("this job type always fails by design");
            case "flaky" -> {
                if (ThreadLocalRandom.current().nextBoolean()) {
                    throw new IllegalStateException("flaky job failed this attempt");
                }
                return "flaky job succeeded on attempt " + job.getAttempts();
            }
            default -> {
                return "echo: " + job.getPayload();
            }
        }
    }
}
