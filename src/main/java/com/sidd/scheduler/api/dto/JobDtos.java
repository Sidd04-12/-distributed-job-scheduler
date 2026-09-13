package com.sidd.scheduler.api.dto;

import com.sidd.scheduler.domain.Job;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.time.Instant;
import java.util.UUID;

public final class JobDtos {

    private JobDtos() {
    }

    /**
     * @param delaySeconds run this many seconds from now; ignored when runAt is supplied
     * @param priority     1 = highest, 9 = lowest; applied among jobs that are already due
     */
    public record SubmitJobRequest(
            @NotBlank String type,
            String payload,
            @Min(1) @Max(9) Integer priority,
            Long delaySeconds,
            Instant runAt,
            @Min(1) @Max(20) Integer maxAttempts
    ) {
    }

    public record JobResponse(
            UUID id,
            String type,
            String payload,
            int priority,
            String status,
            Instant runAt,
            int attempts,
            int maxAttempts,
            String lastError,
            String result,
            String workerId,
            Instant createdAt,
            Instant completedAt
    ) {
        public static JobResponse from(Job j) {
            return new JobResponse(
                    j.getId(), j.getType(), j.getPayload(), j.getPriority(), j.getStatus().name(),
                    j.getRunAt(), j.getAttempts(), j.getMaxAttempts(), j.getLastError(),
                    j.getResult(), j.getWorkerId(), j.getCreatedAt(), j.getCompletedAt());
        }
    }

    /** Published to Kafka. Deliberately small — workers re-read the row for authoritative state. */
    public record JobDispatchedEvent(
            UUID jobId,
            String type,
            int priority,
            int attempt,
            Instant dispatchedAt
    ) {
    }

    public record QueueStats(
            long queueDepth,
            long scheduled,
            long dispatched,
            long running,
            long completed,
            long failed,
            long deadLetter,
            long cancelled
    ) {
    }
}
