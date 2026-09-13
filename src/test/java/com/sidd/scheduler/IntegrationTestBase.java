package com.sidd.scheduler;

import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;

import java.net.InetSocketAddress;
import java.net.Socket;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Base for tests that need real Redis and Postgres.
 * <p>
 * Concurrency guarantees can't be tested against mocks — a mocked Redis will happily report
 * atomicity the real one doesn't provide, and an in-memory database won't reproduce row locking.
 * So these run against the actual services from {@code infra/docker-compose.yml}:
 *
 * <pre>{@code docker compose -f infra/docker-compose.yml up -d postgres redis}</pre>
 *
 * When that stack isn't running the tests skip rather than fail, so {@code mvn test} stays green
 * for anyone who just wants to build. CI brings the services up first.
 */
@SpringBootTest(properties = {
        "scheduler.dispatcher.enabled=false",
        "spring.kafka.listener.auto-startup=false",
        "spring.jpa.hibernate.ddl-auto=none",
        "spring.datasource.url=jdbc:postgresql://localhost:5433/scheduler",
        "spring.datasource.username=scheduler",
        "spring.datasource.password=scheduler",
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6380",
        // Database 1, not 0. The compose stack's dispatcher polls database 0 every 500ms and
        // would happily drain a test's queue mid-assertion — which it did, until this line.
        "spring.data.redis.database=1",
        "spring.kafka.bootstrap-servers=localhost:19093"
})
public abstract class IntegrationTestBase {

    @BeforeAll
    static void requireInfrastructure() {
        assumeTrue(reachable("localhost", 5433),
                "Postgres not reachable on localhost:5433 — start infra/docker-compose.yml to run this test");
        assumeTrue(reachable("localhost", 6380),
                "Redis not reachable on localhost:6380 — start infra/docker-compose.yml to run this test");
    }

    static boolean reachable(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
