package com.sidd.scheduler.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/** Append-only record of a single state transition, so job history is auditable after the fact. */
@Entity
@Table(name = "job_events")
@Getter
@Setter
@NoArgsConstructor
public class JobEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "job_id", nullable = false)
    private UUID jobId;

    @Column(name = "from_status")
    private String fromStatus;

    @Column(name = "to_status", nullable = false)
    private String toStatus;

    @Column(columnDefinition = "text")
    private String detail;

    @Column(name = "worker_id")
    private String workerId;

    @Column(nullable = false)
    private Instant at = Instant.now();

    public static JobEvent of(UUID jobId, JobStatus from, JobStatus to, String detail, String workerId) {
        JobEvent e = new JobEvent();
        e.jobId = jobId;
        e.fromStatus = from == null ? null : from.name();
        e.toStatus = to.name();
        e.detail = detail;
        e.workerId = workerId;
        e.at = Instant.now();
        return e;
    }
}
