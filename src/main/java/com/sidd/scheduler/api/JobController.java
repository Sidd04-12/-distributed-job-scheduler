package com.sidd.scheduler.api;

import com.sidd.scheduler.api.dto.JobDtos.JobResponse;
import com.sidd.scheduler.api.dto.JobDtos.QueueStats;
import com.sidd.scheduler.api.dto.JobDtos.SubmitJobRequest;
import com.sidd.scheduler.domain.Job;
import com.sidd.scheduler.domain.JobEvent;
import com.sidd.scheduler.domain.JobStatus;
import com.sidd.scheduler.queue.JobQueue;
import com.sidd.scheduler.repo.JobRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobService service;
    private final JobRepository jobs;
    private final JobQueue queue;

    @PostMapping
    public ResponseEntity<JobResponse> submit(@Valid @RequestBody SubmitJobRequest request) {
        Job job = service.submit(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(JobResponse.from(job));
    }

    @GetMapping("/{id}")
    public JobResponse get(@PathVariable UUID id) {
        return JobResponse.from(service.get(id));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> cancel(@PathVariable UUID id) {
        boolean cancelled = service.cancel(id);
        return ResponseEntity.status(cancelled ? HttpStatus.OK : HttpStatus.CONFLICT)
                .body(Map.of(
                        "id", id,
                        "cancelled", cancelled,
                        "reason", cancelled ? "cancelled" : "job already running or finished"));
    }

    @GetMapping("/{id}/history")
    public List<JobEvent> history(@PathVariable UUID id) {
        return service.history(id);
    }

    @GetMapping
    public Page<JobResponse> list(
            @RequestParam(required = false) JobStatus status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return service.list(status, PageRequest.of(page, size)).map(JobResponse::from);
    }

    @GetMapping("/stats")
    public QueueStats stats() {
        return new QueueStats(
                queue.size(),
                jobs.countByStatus(JobStatus.SCHEDULED),
                jobs.countByStatus(JobStatus.DISPATCHED),
                jobs.countByStatus(JobStatus.RUNNING),
                jobs.countByStatus(JobStatus.COMPLETED),
                jobs.countByStatus(JobStatus.FAILED),
                jobs.countByStatus(JobStatus.DEAD_LETTER),
                jobs.countByStatus(JobStatus.CANCELLED));
    }
}
