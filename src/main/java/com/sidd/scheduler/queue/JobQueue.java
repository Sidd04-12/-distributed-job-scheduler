package com.sidd.scheduler.queue;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * The pending-job queue, backed by a single Redis sorted set scored by run-at time in epoch
 * millis. Scoring by time (rather than folding priority into the score) keeps delay semantics
 * exact; priority is applied by the dispatcher when ordering the batch of jobs that are already
 * due. Push and pop are both O(log n).
 */
@Component
@RequiredArgsConstructor
public class JobQueue {

    public static final String QUEUE_KEY = "jobs:scheduled";

    /**
     * Reads the due jobs and removes them in a single atomic step.
     * <p>
     * This script is the reason two dispatcher replicas can run safely. Doing the same thing as
     * a ZRANGEBYSCORE followed by a separate ZREM lets a second dispatcher read the same members
     * in the window between the two commands, and the job gets dispatched twice. Redis executes
     * a Lua script atomically, so exactly one caller can ever receive a given member.
     * <p>
     * The LIMIT matters: unpack() on a very large table can overflow the Lua stack, so batches
     * are bounded rather than draining the whole queue at once.
     */
    private static final String CLAIM_DUE_LUA = """
            local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
            if #due > 0 then
                redis.call('ZREM', KEYS[1], unpack(due))
            end
            return due
            """;

    private static final DefaultRedisScript<List> CLAIM_DUE_SCRIPT =
            new DefaultRedisScript<>(CLAIM_DUE_LUA, List.class);

    private final StringRedisTemplate redis;

    /** Schedule a job to become eligible at {@code runAt}. Re-adding an existing id moves it. */
    public void enqueue(UUID jobId, Instant runAt) {
        redis.opsForZSet().add(QUEUE_KEY, jobId.toString(), runAt.toEpochMilli());
    }

    /**
     * Atomically claim up to {@code limit} jobs whose run-at has passed. A claimed job is no
     * longer in the queue, so no other dispatcher can see it.
     */
    @SuppressWarnings("unchecked")
    public List<UUID> claimDue(Instant now, int limit) {
        List<Object> raw = redis.execute(
                CLAIM_DUE_SCRIPT,
                Collections.singletonList(QUEUE_KEY),
                String.valueOf(now.toEpochMilli()),
                String.valueOf(limit));
        if (raw == null || raw.isEmpty()) {
            return List.of();
        }
        return raw.stream().map(Object::toString).map(UUID::fromString).toList();
    }

    /** Remove a job from the queue without dispatching it. Used by cancel. */
    public boolean remove(UUID jobId) {
        Long removed = redis.opsForZSet().remove(QUEUE_KEY, jobId.toString());
        return removed != null && removed > 0;
    }

    public long size() {
        Long n = redis.opsForZSet().zCard(QUEUE_KEY);
        return n == null ? 0 : n;
    }
}
