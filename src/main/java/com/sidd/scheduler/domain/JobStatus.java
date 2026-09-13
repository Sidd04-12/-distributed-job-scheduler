package com.sidd.scheduler.domain;

public enum JobStatus {
    /** Waiting in the Redis queue for its run_at to arrive. */
    SCHEDULED,
    /** Claimed off the queue and published to Kafka; not yet picked up by a worker. */
    DISPATCHED,
    /** A worker has won the idempotency guard and is executing it. */
    RUNNING,
    COMPLETED,
    /** Execution failed but retries remain; it goes back to SCHEDULED with a backoff delay. */
    FAILED,
    /** Retries exhausted. Published to the dead-letter topic and left for inspection. */
    DEAD_LETTER,
    CANCELLED
}
