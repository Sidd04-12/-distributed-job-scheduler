package com.sidd.scheduler.config;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topics explicitly rather than relying on broker auto-creation.
 * <p>
 * This matters more than it looks. An auto-created topic gets <b>one</b> partition, and a Kafka
 * consumer group can never have more actively-consuming members than there are partitions — the
 * extras are assigned nothing and sit idle. With one partition, running ten worker replicas
 * gets you the throughput of one, which would make this project's whole premise false.
 * <p>
 * Partition count is therefore the real ceiling on worker parallelism, and it can only be raised
 * later, never lowered. Twelve is a deliberate over-provision: far more than the workers this
 * runs with today, so the pool can grow without repartitioning.
 */
@Configuration
@RequiredArgsConstructor
public class KafkaTopicConfig {

    private static final int READY_PARTITIONS = 12;
    private static final int DLQ_PARTITIONS = 3;

    private final SchedulerProperties props;

    @Bean
    public NewTopic jobsReadyTopic() {
        return TopicBuilder.name(props.getTopics().getReady())
                .partitions(READY_PARTITIONS)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic jobsDlqTopic() {
        return TopicBuilder.name(props.getTopics().getDlq())
                .partitions(DLQ_PARTITIONS)
                .replicas(1)
                .build();
    }
}
