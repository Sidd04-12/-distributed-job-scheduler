-- Durable job ledger. Redis holds the pending queue for fast scheduling, but Postgres is the
-- system of record: if Redis is wiped, jobs can be rebuilt from here.
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS jobs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    type            TEXT        NOT NULL,
    payload         TEXT        NOT NULL DEFAULT '{}',
    priority        INTEGER     NOT NULL DEFAULT 5,
    status          TEXT        NOT NULL DEFAULT 'SCHEDULED',
    -- SCHEDULED -> DISPATCHED -> RUNNING -> COMPLETED
    --                        \-> FAILED -> SCHEDULED (retry) -> ... -> DEAD_LETTER
    -- SCHEDULED/DISPATCHED -> CANCELLED
    run_at          TIMESTAMPTZ NOT NULL,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    max_attempts    INTEGER     NOT NULL DEFAULT 5,
    last_error      TEXT,
    result          TEXT,
    worker_id       TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    CONSTRAINT jobs_priority_range CHECK (priority BETWEEN 1 AND 9)
);

CREATE INDEX IF NOT EXISTS idx_jobs_status  ON jobs (status);
CREATE INDEX IF NOT EXISTS idx_jobs_run_at  ON jobs (run_at) WHERE status = 'SCHEDULED';
CREATE INDEX IF NOT EXISTS idx_jobs_type    ON jobs (type);

-- Append-only audit of every state transition. Makes the "did this job run twice?"
-- question answerable after the fact rather than a matter of trust.
CREATE TABLE IF NOT EXISTS job_events (
    id          BIGSERIAL PRIMARY KEY,
    job_id      UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    from_status TEXT,
    to_status   TEXT        NOT NULL,
    detail      TEXT,
    worker_id   TEXT,
    at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_job_events_job ON job_events (job_id, at);
