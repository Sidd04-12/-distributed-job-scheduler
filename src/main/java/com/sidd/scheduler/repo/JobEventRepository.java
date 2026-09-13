package com.sidd.scheduler.repo;

import com.sidd.scheduler.domain.JobEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface JobEventRepository extends JpaRepository<JobEvent, Long> {
    List<JobEvent> findByJobIdOrderByAtAsc(UUID jobId);
}
