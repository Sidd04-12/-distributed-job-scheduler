# Distributed Job Scheduler

A job scheduler that runs work later, in priority order, across a pool of workers that scale
independently of whoever submitted the job. Java 17 / Spring Boot, with Redis for the pending
queue, Kafka for dispatch, and Postgres as the durable record.

The interesting part isn't the API — it's what happens when two dispatchers race for the same
job, when Kafka delivers the same message twice, or when someone cancels a job in the
microsecond it's being handed to a worker. Those three cases are what the design is actually
about, and each has a test that proves the behaviour rather than asserting it.

## How it works

```
POST /api/v1/jobs ──▶ scheduler-api
                          ├─ Postgres  · durable job row (written first)
                          └─ Redis ZSET · score = run_at epoch millis
                                    │
                              dispatcher · polls for due jobs
                                    │  ⚠ atomic claim via Lua
                                    ▼
                             [ jobs.ready ] Kafka
                                    ▼
                        worker pool · one consumer group
                             ├─ ok        ─▶ COMPLETED
                             ├─ fail      ─▶ backoff + jitter, re-enqueue
                             └─ exhausted ─▶ [ jobs.dlq ]
```

A job's lifecycle is a state machine, and every transition is written to an append-only
`job_events` table, so "did this job run twice?" is answerable from data rather than trust:

```
SCHEDULED ─▶ DISPATCHED ─▶ RUNNING ─▶ COMPLETED
     ▲            │            │
     └────────────┴────────────┴─▶ FAILED ─▶ (retry) ─▶ SCHEDULED
                                       └─▶ DEAD_LETTER
     └─▶ CANCELLED
```

## The four problems worth explaining

### 1. Two dispatchers, one job

The obvious implementation — `ZRANGEBYSCORE` to find due jobs, then `ZREM` to take them —
is wrong. Between those two commands, a second dispatcher can run the same range query and get
the same members. Both dispatch. The job runs twice.

The fix is to make finding-and-removing a single atomic operation. Redis executes Lua scripts
atomically, so no other client can observe the intermediate state:

```lua
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #due > 0 then
    redis.call('ZREM', KEYS[1], unpack(due))
end
return due
```

Exactly one caller can ever receive a given member. `JobQueueConcurrencyTest` enqueues 2,000
jobs, drains them from 8 concurrent threads, and asserts every job was claimed exactly once.

The `LIMIT` isn't cosmetic — `unpack()` on a very large table can overflow the Lua stack, so
batches are bounded rather than draining the queue in one call.

### 2. Kafka delivers at-least-once

Consumer rebalances and lost offset commits mean a worker *will* occasionally receive the same
job twice. Exactly-once delivery isn't available, so the system doesn't pretend otherwise —
it makes duplicate *delivery* harmless by making *execution* conditional:

```sql
UPDATE jobs SET status = 'RUNNING', worker_id = ?, attempts = attempts + 1
 WHERE id = ? AND status = 'DISPATCHED'
```

The first delivery matches the row and transitions it. The second matches nothing, gets `0`
rows back, and returns without executing. The database's row lock provides the mutual exclusion,
which is why this holds across any number of workers on any number of machines — no distributed
lock, no lease, no coordination service.

`IdempotencyGuardTest` throws 16 concurrent threads at one job — each in its own transaction,
because the conflict being guarded against is *between* transactions — and asserts exactly one
wins and the attempt counter increments exactly once.

### 3. Cancelling a job that's already moving

Cancel is inherently racy: the job may be sitting in Redis, may be in flight on a Kafka topic,
or may already be executing. Rather than trying to catch it everywhere, cancellation reuses the
same guard. Cancel removes it from Redis and writes `CANCELLED`; since `CANCELLED` is not
`DISPATCHED`, any worker that later picks it up fails its conditional claim and never runs it.

The race resolves deterministically whichever way it interleaves — and exactly one of the two
operations reports success. `concurrentCancelAndClaimProduceExactlyOneOutcome` runs that race
150 times and asserts `cancel + claim == 1` every round.

### 4. Partition count is the real scaling ceiling

"Horizontally scalable worker pool" is only true if Kafka actually has work to hand each worker.
A consumer group can never have more *actively consuming* members than the topic has partitions —
extras are assigned nothing and sit idle. An auto-created topic gets **one** partition, so ten
worker replicas would give you the throughput of one.

This isn't hypothetical: it's exactly what happened the first time this ran. Five consumers
joined the group and four got `partitions assigned: []`. So the topics are declared explicitly
with `NewTopic` beans instead of being auto-created — 12 partitions for `jobs.ready`, which is
deliberate over-provisioning, because partition count can be raised later but never lowered.

Verified with three worker replicas plus the API's embedded worker:

```
$ rpk group describe job-workers
MEMBERS  9
jobs.ready  0   ... 172.19.0.6
jobs.ready  2   ... 172.19.0.8
jobs.ready  4   ... 172.19.0.7
jobs.ready  6   ... 172.19.0.5    ← all 12 partitions spread
jobs.ready  9   ... 172.19.0.7       across 4 containers
jobs.ready  11  ... 172.19.0.5
```

## Priority and delay

One sorted set, scored by run-at time in epoch millis. Priority is applied by the dispatcher
across the batch of jobs that are *already due*, rather than folded into the score.

That ordering matters: folding priority into the score distorts scheduled times, so a
high-priority job scheduled for tomorrow could jump ahead of a low-priority job due now. Sorting
the due batch keeps delays exact while still letting a priority-1 job overtake a priority-9 one
that came due in the same tick. Push and pop stay O(log n).

## Retries

Exponential backoff — `base × 2^(attempt-1)`, capped — plus up to 20% jitter. The jitter isn't
decoration: a batch of jobs that failed together against the same dependency would otherwise
retry in lockstep and stampede it the moment it came back. After `maxAttempts`, the job is
dead-lettered to `jobs.dlq` and left in Postgres for inspection.

## Running it

```bash
cd infra
docker compose up --build -d
./demo.sh
```

`demo.sh` walks through priority ordering, a delayed job, a cancellation, and a job that fails
its way into the dead-letter topic.

Scale the worker pool and watch Kafka rebalance partitions across the new members — no code
change, no restart of anything else:

```bash
docker compose up -d --scale worker=4
```

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/v1/jobs` | Submit a job |
| GET | `/api/v1/jobs/{id}` | Job detail |
| DELETE | `/api/v1/jobs/{id}` | Cancel (409 if already running or finished) |
| GET | `/api/v1/jobs/{id}/history` | Every state transition for this job |
| GET | `/api/v1/jobs?status=&page=&size=` | List / filter |
| GET | `/api/v1/jobs/stats` | Queue depth and counts by status |

```bash
# run in 30 seconds, high priority, give up after 3 attempts
curl -X POST http://localhost:8082/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"echo","payload":"hello","priority":1,"delaySeconds":30,"maxAttempts":3}'
```

Job types are deliberately simple stand-ins so the scheduler's own behaviour can be exercised
deterministically: `echo` succeeds, `sleep` takes 250ms, `flaky` fails about half the time, and
`always-fail` always throws.

## Tests

```bash
# Concurrency tests need real Redis and Postgres — mocks would "prove" atomicity
# that the real system doesn't have.
docker compose -f infra/docker-compose.yml up -d postgres redis
./mvnw test
```

11 tests. They skip with a clear message rather than failing if the infrastructure isn't up, so
a plain `mvn test` still works for anyone who just wants to build.

Tests run on Redis database 1 while the application uses database 0. That isolation is load-
bearing: sharing a database meant the running dispatcher drained the test's queue mid-assertion,
which showed up as a test expecting 2,000 queued jobs and finding 1,532.

| Test | What it proves |
|---|---|
| `everyJobIsClaimedExactlyOnceUnderConcurrentDispatchers` | 2,000 jobs, 8 threads, zero double-claims |
| `jobsAreNotClaimedBeforeTheyAreDue` | Delay is respected |
| `onlyOneOfManyConcurrentClaimsWins` | 16 workers race one job, one wins |
| `duplicateDeliveryAfterCompletionDoesNotReRun` | Re-delivery after completion is a no-op |
| `concurrentCancelAndClaimProduceExactlyOneOutcome` | 150 rounds of the cancel/dispatch race |
| `backoffGrowsExponentially` / `backoffIsCappedAtMax` / `jitterVariesBetweenCalls` | Retry timing |

## What this doesn't do

Worth being straight about the limits:

- **Throughput is unmeasured.** There's no benchmark in this repo, so there's no number here.
- **A worker that dies mid-execution leaves its job in `RUNNING`.** Recovering those needs a
  lease with a heartbeat and a reaper for expired leases. The `job_events` table already records
  enough to find them; the reaper isn't written.
- **Job payloads are opaque strings** and the executor is a stand-in, not a plugin system.
- **No authentication** on the API.

## Stack

Java 17 · Spring Boot 3.3 · PostgreSQL 16 · Redis 7 · Redpanda (Kafka API) · Docker Compose
