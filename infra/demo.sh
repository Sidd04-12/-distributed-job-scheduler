#!/usr/bin/env bash
# Walks the scheduler through its interesting paths: priority ordering, delayed execution,
# cancellation, retry with backoff, and dead-lettering.
set -euo pipefail

API="${API:-http://localhost:8082}"

submit() {
  curl -sf -X POST "$API/api/v1/jobs" -H "Content-Type: application/json" -d "$1" \
    | python -c "import sys,json; print(json.load(sys.stdin)['id'])"
}

echo "Waiting for the scheduler..."
for _ in $(seq 1 30); do
  curl -sf "$API/actuator/health" > /dev/null && break
  sleep 2
done

echo ""
echo "1. Priority — three jobs all due now, submitted worst-priority first."
LOW=$(submit '{"type":"echo","payload":"priority 9","priority":9}')
MID=$(submit '{"type":"echo","payload":"priority 5","priority":5}')
HIGH=$(submit '{"type":"echo","payload":"priority 1","priority":1}')
echo "   submitted: p9=$LOW  p5=$MID  p1=$HIGH"
echo "   the dispatcher orders the due batch by priority, so p1 goes to Kafka first."

echo ""
echo "2. Delay — a job that should not run for 30 seconds."
DELAYED=$(submit '{"type":"echo","payload":"see you in 30s","delaySeconds":30}')
echo "   submitted: $DELAYED"

echo ""
echo "3. Cancellation — schedule one, then cancel it before it comes due."
DOOMED=$(submit '{"type":"echo","payload":"never runs","delaySeconds":20}')
sleep 1
curl -sf -X DELETE "$API/api/v1/jobs/$DOOMED" | python -m json.tool
echo "   it is now out of the Redis queue AND marked CANCELLED, so even a dispatch"
echo "   already in flight would fail its conditional claim."

echo ""
echo "4. Retry and dead-letter — a job type that always throws, 3 attempts allowed."
DLQ=$(submit '{"type":"always-fail","payload":"doomed","maxAttempts":3}')
echo "   submitted: $DLQ — watch it back off 1s, 2s, 4s then dead-letter."

echo ""
echo "Waiting 12s for the queue to work through..."
sleep 12

echo ""
echo "Queue stats:"
curl -sf "$API/api/v1/jobs/stats" | python -m json.tool

echo ""
echo "History of the failing job (every state transition is recorded):"
curl -sf "$API/api/v1/jobs/$DLQ/history" | python -m json.tool

echo ""
echo "The delayed job is still waiting, as scheduled:"
curl -sf "$API/api/v1/jobs/$DELAYED" | python -m json.tool
