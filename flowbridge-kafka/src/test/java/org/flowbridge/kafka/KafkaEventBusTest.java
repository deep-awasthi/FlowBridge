package org.flowbridge.kafka;

import org.flowbridge.core.application.service.DefaultEventBus;
import org.flowbridge.core.infrastructure.serialization.ProtostuffSerializer;
import org.flowbridge.core.infrastructure.serialization.Serializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the Kafka provider using Testcontainers.
 *
 * <p>A real Kafka broker is started in Docker for the duration of the test class.
 * These tests verify the full publish-subscribe cycle end-to-end through FlowBridge's
 * Kafka adapter without any mocking.
 *
 * <p><strong>Requires Docker</strong> to be running on the host machine.
 */
@DisplayName("Kafka Provider Integration Tests")
class KafkaEventBusTest {

    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0")
    );

    @BeforeAll
    static void startContainer() {
        try {
            Assumptions.assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                    "Docker daemon is not running. Skipping Kafka integration tests.");
            kafka.start();
        } catch (Throwable t) {
            Assumptions.assumeTrue(false,
                    "Docker check failed or container failed to start. Skipping Kafka integration tests. Reason: " + t.getMessage());
        }
    }

    @AfterAll
    static void stopContainer() {
        try {
            if (kafka.isRunning()) {
                kafka.stop();
            }
        } catch (Throwable t) {
            // Ignore during shutdown
        }
    }

    // Shared fixtures
    private Serializer serializer;
    private KafkaEventRegistry registry;
    private KafkaPublisher publisher;
    private KafkaSubscriber subscriber;
    private KafkaConsumerLoop consumerLoop;
    private DefaultEventBus eventBus;

    // Payload used across tests
    public static class OrderPayload {
        private String orderId;
        public OrderPayload() {}
        public OrderPayload(String orderId) { this.orderId = orderId; }
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
    }

    @BeforeEach
    void setUp() {
        serializer   = new ProtostuffSerializer();
        registry     = new KafkaEventRegistry();
        publisher    = new KafkaPublisher(kafka.getBootstrapServers(), "");
        subscriber   = new KafkaSubscriber(registry, "");
        consumerLoop = new KafkaConsumerLoop(kafka.getBootstrapServers(), "test-group-" + System.nanoTime(), registry);
        eventBus     = new DefaultEventBus(publisher, subscriber, serializer);
    }

    @AfterEach
    void tearDown() throws Exception {
        consumerLoop.close();
        publisher.close();
    }

    @Test
    @DisplayName("publish and subscribe — message is delivered end-to-end via Kafka")
    void publishSubscribe_deliversMessageViaKafka() throws Exception {
        List<OrderPayload> received = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        // Register subscriber BEFORE starting the consumer loop
        eventBus.subscribe("orders", OrderPayload.class, payload -> {
            received.add(payload);
            latch.countDown();
        });

        // Start consumer loop after subscription is registered
        consumerLoop.startOnVirtualThread();

        // Allow consumer group to initialize and join
        Thread.sleep(3000);

        // Publish the event
        eventBus.publish("orders", new OrderPayload("ORD-KAFKA-001"));

        // Wait for delivery (Kafka end-to-end latency is typically < 2s in local Docker)
        boolean delivered = latch.await(15, TimeUnit.SECONDS);

        assertTrue(delivered, "Event was not delivered via Kafka within timeout");
        assertEquals(1, received.size());
        assertEquals("ORD-KAFKA-001", received.get(0).getOrderId());
    }

    @Test
    @DisplayName("multiple subscribers — all receive the same event")
    void multipleSubscribers_allReceiveEvent() throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        List<String> subscriber1Events = new ArrayList<>();
        List<String> subscriber2Events = new ArrayList<>();

        eventBus.subscribe("notifications", OrderPayload.class, p -> {
            subscriber1Events.add(p.getOrderId());
            latch.countDown();
        });
        eventBus.subscribe("notifications", OrderPayload.class, p -> {
            subscriber2Events.add(p.getOrderId());
            latch.countDown();
        });

        consumerLoop.startOnVirtualThread();
        Thread.sleep(3000);

        eventBus.publish("notifications", new OrderPayload("ORD-MULTI-001"));

        boolean delivered = latch.await(15, TimeUnit.SECONDS);

        assertTrue(delivered, "Not all subscribers received the event");
        assertEquals(List.of("ORD-MULTI-001"), subscriber1Events);
        assertEquals(List.of("ORD-MULTI-001"), subscriber2Events);
    }

    @Test
    @DisplayName("topic prefix — messages routed to prefixed Kafka topic")
    void topicPrefix_routesCorrectly() throws Exception {
        String prefix = "myapp";
        KafkaPublisher prefixedPublisher   = new KafkaPublisher(kafka.getBootstrapServers(), prefix);
        KafkaSubscriber prefixedSubscriber = new KafkaSubscriber(registry, prefix);
        KafkaConsumerLoop prefixedLoop     = new KafkaConsumerLoop(
                kafka.getBootstrapServers(), "prefix-group-" + System.nanoTime(), registry);

        try {
            DefaultEventBus prefixedBus = new DefaultEventBus(prefixedPublisher, prefixedSubscriber, serializer);

            CountDownLatch latch = new CountDownLatch(1);
            List<OrderPayload> received = new ArrayList<>();

            prefixedBus.subscribe("payments", OrderPayload.class, p -> {
                received.add(p);
                latch.countDown();
            });

            prefixedLoop.startOnVirtualThread();
            Thread.sleep(3000);

            prefixedBus.publish("payments", new OrderPayload("PAY-PREFIX-001"));

            boolean delivered = latch.await(15, TimeUnit.SECONDS);

            assertTrue(delivered, "Prefixed event not delivered");
            assertEquals("PAY-PREFIX-001", received.get(0).getOrderId());
        } finally {
            prefixedLoop.close();
            prefixedPublisher.close();
        }
    }
}
