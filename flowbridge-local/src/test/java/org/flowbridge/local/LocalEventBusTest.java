package org.flowbridge.local;

import org.flowbridge.core.application.port.in.EventBus;
import org.flowbridge.core.application.service.DefaultEventBus;
import org.flowbridge.core.infrastructure.serialization.ProtostuffSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class LocalEventBusTest {

    private LocalEventRegistry registry;
    private LocalPublisher publisher;
    private LocalSubscriber subscriber;
    private EventBus eventBus;

    public static class SamplePayload {
        private String message;
        private int code;

        public SamplePayload() {}

        public SamplePayload(String message, int code) {
            this.message = message;
            this.code = code;
        }

        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        public int getCode() { return code; }
        public void setCode(int code) { this.code = code; }
    }

    @BeforeEach
    public void setUp() {
        registry = new LocalEventRegistry();
        publisher = new LocalPublisher(registry);
        subscriber = new LocalSubscriber(registry);
        eventBus = new DefaultEventBus(publisher, subscriber, new ProtostuffSerializer());
    }

    @AfterEach
    public void tearDown() {
        publisher.close();
    }

    @Test
    public void testPublishAndSubscribeAsync() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SamplePayload> receivedPayload = new AtomicReference<>();
        AtomicBoolean wasVirtualThread = new AtomicBoolean(false);

        eventBus.subscribe("test-topic", SamplePayload.class, payload -> {
            receivedPayload.set(payload);
            wasVirtualThread.set(Thread.currentThread().isVirtual());
            latch.countDown();
        });

        SamplePayload sent = new SamplePayload("Hello FlowBridge", 200);
        eventBus.publish("test-topic", sent);

        boolean finished = latch.await(2, TimeUnit.SECONDS);
        assertTrue(finished, "Callback was not invoked in time");
        assertNotNull(receivedPayload.get());
        assertEquals("Hello FlowBridge", receivedPayload.get().getMessage());
        assertEquals(200, receivedPayload.get().getCode());
        assertTrue(wasVirtualThread.get(), "Expected execution on a Java 21 Virtual Thread");
    }

    @Test
    public void testMultipleSubscribers() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicInteger invocationCount = new AtomicInteger(0);

        eventBus.subscribe("test-topic-multi", SamplePayload.class, payload -> {
            invocationCount.incrementAndGet();
            latch.countDown();
        });

        eventBus.subscribe("test-topic-multi", SamplePayload.class, payload -> {
            invocationCount.incrementAndGet();
            latch.countDown();
        });

        SamplePayload sent = new SamplePayload("Multi test", 1);
        eventBus.publish("test-topic-multi", sent);

        boolean finished = latch.await(2, TimeUnit.SECONDS);
        assertTrue(finished, "Both callbacks were not invoked in time");
        assertEquals(2, invocationCount.get());
    }

    @Test
    public void testSubscriberExceptionIsolation() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean secondSubscriberInvoked = new AtomicBoolean(false);

        // First subscriber throws an exception
        eventBus.subscribe("test-topic-fail", SamplePayload.class, payload -> {
            throw new RuntimeException("Simulated subscriber error");
        });

        // Second subscriber should still run
        eventBus.subscribe("test-topic-fail", SamplePayload.class, payload -> {
            secondSubscriberInvoked.set(true);
            latch.countDown();
        });

        SamplePayload sent = new SamplePayload("Fail isolation test", 99);
        eventBus.publish("test-topic-fail", sent);

        boolean finished = latch.await(2, TimeUnit.SECONDS);
        assertTrue(finished, "Second subscriber was not invoked");
        assertTrue(secondSubscriberInvoked.get());
    }

    @Test
    public void testTopicIsolation() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean receivedOnWrongTopic = new AtomicBoolean(false);

        eventBus.subscribe("topic-a", SamplePayload.class, payload -> {
            latch.countDown();
        });

        eventBus.subscribe("topic-b", SamplePayload.class, payload -> {
            receivedOnWrongTopic.set(true);
        });

        SamplePayload sent = new SamplePayload("Message for A", 10);
        eventBus.publish("topic-a", sent);

        boolean finished = latch.await(2, TimeUnit.SECONDS);
        assertTrue(finished);
        assertFalse(receivedOnWrongTopic.get());
    }
}
