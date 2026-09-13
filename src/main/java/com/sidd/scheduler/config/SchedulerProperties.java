package com.sidd.scheduler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {

    private final Topics topics = new Topics();
    private final DispatcherProps dispatcher = new DispatcherProps();
    private final Retry retry = new Retry();

    public Topics getTopics() { return topics; }
    public DispatcherProps getDispatcher() { return dispatcher; }
    public Retry getRetry() { return retry; }

    public static class Topics {
        private String ready = "jobs.ready";
        private String dlq = "jobs.dlq";

        public String getReady() { return ready; }
        public void setReady(String ready) { this.ready = ready; }
        public String getDlq() { return dlq; }
        public void setDlq(String dlq) { this.dlq = dlq; }
    }

    public static class DispatcherProps {
        private boolean enabled = true;
        private long pollIntervalMs = 500;
        /** Bounded so the Lua claim's unpack() never overflows the Redis stack. */
        private int batchSize = 100;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public long getPollIntervalMs() { return pollIntervalMs; }
        public void setPollIntervalMs(long pollIntervalMs) { this.pollIntervalMs = pollIntervalMs; }
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
    }

    public static class Retry {
        private long baseBackoffMs = 1000;
        private long maxBackoffMs = 60000;

        public long getBaseBackoffMs() { return baseBackoffMs; }
        public void setBaseBackoffMs(long baseBackoffMs) { this.baseBackoffMs = baseBackoffMs; }
        public long getMaxBackoffMs() { return maxBackoffMs; }
        public void setMaxBackoffMs(long maxBackoffMs) { this.maxBackoffMs = maxBackoffMs; }
    }
}
