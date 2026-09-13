package com.sidd.scheduler;

import com.sidd.scheduler.queue.JobQueue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The claim in the README is that two dispatchers can never hand the same job to two workers.
 * That is a property of the Lua script, and the only honest way to test it is against a real
 * Redis with real concurrent callers.
 */
class JobQueueConcurrencyTest extends IntegrationTestBase {

    @Autowired
    JobQueue queue;

    @Autowired
    StringRedisTemplate redisTemplate;

    @BeforeEach
    void clearQueue() {
        redisTemplate.delete(JobQueue.QUEUE_KEY);
    }

    @Test
    void everyJobIsClaimedExactlyOnceUnderConcurrentDispatchers() throws Exception {
        int jobCount = 2000;
        int dispatchers = 8;

        Instant past = Instant.now().minusSeconds(60);
        for (int i = 0; i < jobCount; i++) {
            queue.enqueue(UUID.randomUUID(), past);
        }
        assertEquals(jobCount, queue.size());

        ExecutorService pool = Executors.newFixedThreadPool(dispatchers);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<List<UUID>>> futures = new java.util.ArrayList<>();

        for (int d = 0; d < dispatchers; d++) {
            futures.add(pool.submit(() -> {
                start.await();
                List<UUID> mine = new java.util.ArrayList<>();
                // Drain until the queue is empty from this dispatcher's point of view.
                for (int attempt = 0; attempt < 500; attempt++) {
                    List<UUID> claimed = queue.claimDue(Instant.now(), 50);
                    if (claimed.isEmpty()) {
                        if (queue.size() == 0) break;
                        continue;
                    }
                    mine.addAll(claimed);
                }
                return mine;
            }));
        }

        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "dispatchers did not finish in time");

        Set<UUID> seen = new HashSet<>();
        AtomicInteger total = new AtomicInteger();
        for (Future<List<UUID>> f : futures) {
            for (UUID id : f.get()) {
                total.incrementAndGet();
                assertTrue(seen.add(id), "job " + id + " was claimed by more than one dispatcher");
            }
        }

        assertEquals(jobCount, total.get(), "some jobs were never claimed");
        assertEquals(jobCount, seen.size());
        assertEquals(0, queue.size(), "queue should be drained");
    }

    @Test
    void jobsAreNotClaimedBeforeTheyAreDue() {
        UUID future = UUID.randomUUID();
        UUID ready = UUID.randomUUID();

        queue.enqueue(future, Instant.now().plusSeconds(3600));
        queue.enqueue(ready, Instant.now().minusSeconds(1));

        List<UUID> claimed = queue.claimDue(Instant.now(), 100);

        assertEquals(List.of(ready), claimed);
        assertEquals(1, queue.size(), "the future-dated job should still be queued");
    }

    @Test
    void removeTakesAJobOutOfTheQueue() {
        UUID id = UUID.randomUUID();
        queue.enqueue(id, Instant.now().minusSeconds(1));

        assertTrue(queue.remove(id));
        assertTrue(queue.claimDue(Instant.now(), 10).isEmpty());
        assertFalse(queue.remove(id), "removing twice should report nothing removed");
    }
}
