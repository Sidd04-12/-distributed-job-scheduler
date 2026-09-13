package com.sidd.scheduler;

import com.sidd.scheduler.config.SchedulerProperties;
import com.sidd.scheduler.worker.JobOutcomeHandler;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackoffTest {

    private JobOutcomeHandler handlerWith(long base, long max) {
        SchedulerProperties props = new SchedulerProperties();
        props.getRetry().setBaseBackoffMs(base);
        props.getRetry().setMaxBackoffMs(max);
        return new JobOutcomeHandler(null, null, null, null, props);
    }

    @Test
    void backoffGrowsExponentially() {
        JobOutcomeHandler h = handlerWith(1000, 600_000);
        long a1 = h.backoffFor(1).toMillis();
        long a2 = h.backoffFor(2).toMillis();
        long a3 = h.backoffFor(3).toMillis();

        // Each attempt at least doubles the base delay before jitter is added.
        assertTrue(a1 >= 1000 && a1 < 1200, "attempt 1 was " + a1);
        assertTrue(a2 >= 2000 && a2 < 2400, "attempt 2 was " + a2);
        assertTrue(a3 >= 4000 && a3 < 4800, "attempt 3 was " + a3);
    }

    @Test
    void backoffIsCappedAtMax() {
        JobOutcomeHandler h = handlerWith(1000, 5000);
        // Attempt 10 would be ~512s uncapped; the cap plus <=20% jitter bounds it.
        long delay = h.backoffFor(10).toMillis();
        assertTrue(delay >= 5000 && delay <= 6000, "expected capped delay, got " + delay);
    }

    @Test
    void jitterVariesBetweenCalls() {
        JobOutcomeHandler h = handlerWith(10_000, 600_000);
        boolean sawDifference = false;
        long first = h.backoffFor(3).toMillis();
        for (int i = 0; i < 40 && !sawDifference; i++) {
            if (h.backoffFor(3).toMillis() != first) {
                sawDifference = true;
            }
        }
        // Without jitter, a batch of jobs that failed together would retry in lockstep.
        assertTrue(sawDifference, "backoff produced identical delays — jitter is not being applied");
    }

    @Test
    void firstAttemptDoesNotUnderflow() {
        JobOutcomeHandler h = handlerWith(1000, 600_000);
        assertEquals(Duration.ofMillis(1000).toMillis(), h.backoffFor(0).toMillis() / 1000 * 1000);
    }
}
