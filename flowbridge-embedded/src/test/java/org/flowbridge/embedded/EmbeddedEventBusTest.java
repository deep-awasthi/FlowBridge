package org.flowbridge.embedded;

import org.flowbridge.core.application.port.in.EventBus;
import org.flowbridge.core.application.service.DefaultEventBus;
import org.flowbridge.core.domain.model.DeadLetterRecord;
import org.flowbridge.core.domain.model.Event;
import org.flowbridge.core.domain.model.ReplayOptions;
import org.flowbridge.core.infrastructure.serialization.ProtostuffSerializer;
import org.flowbridge.core.infrastructure.serialization.Serializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class EmbeddedEventBusTest {

    @TempDir
    Path tempDir;

    private RocksDBManager dbManager;
    private RocksDBReplayableStore replayableStore;
    private RocksDBDeadLetterStore deadLetterStore;
    private Serializer serializer;
    
    private EmbeddedEventRegistry registry;
    private EmbeddedPublisher publisher;
    private EmbeddedSubscriber subscriber;
    private EventBus eventBus;

    public static class DemoPayload {
        private String content;
        private int priority;

        public DemoPayload() {}

        public DemoPayload(String content, int priority) {
            this.content = content;
            this.priority = priority;
        }

        public String getContent() { return content; }
        public void setContent(String content) { this.content = content; }
        public int getPriority() { return priority; }
        public void setPriority(int priority) { this.priority = priority; }
    }

    @BeforeEach
    public void setUp() {
        String dbPath = tempDir.resolve("flowbridge-db").toString();
        serializer = new ProtostuffSerializer();
        dbManager = new RocksDBManager(dbPath);
        replayableStore = new RocksDBReplayableStore(dbManager, serializer);
        deadLetterStore = new RocksDBDeadLetterStore(dbManager, serializer);
        
        registry = new EmbeddedEventRegistry();
        publisher = new EmbeddedPublisher(registry);
        subscriber = new EmbeddedSubscriber(registry);
        
        eventBus = new DefaultEventBus(publisher, subscriber, serializer, replayableStore, deadLetterStore);
    }

    @AfterEach
    public void tearDown() {
        if (publisher != null) {
            publisher.close();
        }
        if (dbManager != null) {
            dbManager.close();
        }
    }

    @Test
    public void testPublishAndSubscribeAsync() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        List<DemoPayload> received = new ArrayList<>();
        AtomicBoolean isVirtualThread = new AtomicBoolean(false);

        eventBus.subscribe("embedded-test", DemoPayload.class, payload -> {
            received.add(payload);
            isVirtualThread.set(Thread.currentThread().isVirtual());
            latch.countDown();
        });

        DemoPayload payload = new DemoPayload("Embedded works!", 1);
        eventBus.publish("embedded-test", payload);

        boolean finished = latch.await(2, TimeUnit.SECONDS);
        assertTrue(finished);
        assertEquals(1, received.size());
        assertEquals("Embedded works!", received.get(0).getContent());
        assertTrue(isVirtualThread.get(), "Expected delivery to happen on Virtual Threads");
    }

    @Test
    public void testPersistenceAcrossRestarts() {
        // Publish event to database
        DemoPayload payload = new DemoPayload("Persistent Event", 5);
        eventBus.publish("restart-topic", payload);

        // Fetch events before restart to verify they are stored
        List<Event> storedEventsBefore = replayableStore.findByTopic("restart-topic");
        assertEquals(1, storedEventsBefore.size());
        assertEquals(1L, storedEventsBefore.get(0).getOffset());

        // Shutdown database manager
        dbManager.close();

        // Reopen database using the same path
        String dbPath = tempDir.resolve("flowbridge-db").toString();
        dbManager = new RocksDBManager(dbPath);
        replayableStore = new RocksDBReplayableStore(dbManager, serializer);

        // Fetch events after restart and verify it was preserved
        List<Event> storedEventsAfter = replayableStore.findByTopic("restart-topic");
        assertEquals(1, storedEventsAfter.size());
        Event event = storedEventsAfter.get(0);
        assertEquals(1L, event.getOffset());
        assertEquals("restart-topic", event.getTopic());
        
        DemoPayload deserialized = serializer.deserialize(event.getPayload(), DemoPayload.class);
        assertEquals("Persistent Event", deserialized.getContent());
        assertEquals(5, deserialized.getPriority());
    }

    @Test
    public void testEventReplay() {
        // Manually write events with specific offsets/timestamps to ensure predictable values
        long now = System.currentTimeMillis();

        Event event1 = new Event("1", "replay-topic", serializer.serialize(new DemoPayload("Msg 1", 1)),
                DemoPayload.class.getName(), now - 10000, 0, new HashMap<>());
        Event event2 = new Event("2", "replay-topic", serializer.serialize(new DemoPayload("Msg 2", 2)),
                DemoPayload.class.getName(), now - 5000, 0, new HashMap<>());
        Event event3 = new Event("3", "replay-topic", serializer.serialize(new DemoPayload("Msg 3", 3)),
                DemoPayload.class.getName(), now, 0, new HashMap<>());

        replayableStore.save(event1);
        replayableStore.save(event2);
        replayableStore.save(event3);

        // Subscribe to replay-topic
        List<DemoPayload> replayedPayloads = new ArrayList<>();
        eventBus.subscribe("replay-topic", DemoPayload.class, replayedPayloads::add);

        // 1. Replay ALL
        replayedPayloads.clear();
        eventBus.replay("replay-topic", ReplayOptions.all());
        assertEquals(3, replayedPayloads.size());
        assertEquals("Msg 1", replayedPayloads.get(0).getContent());
        assertEquals("Msg 2", replayedPayloads.get(1).getContent());
        assertEquals("Msg 3", replayedPayloads.get(2).getContent());

        // 2. Replay from Timestamp (since now - 7000ms, should fetch event2 and event3)
        replayedPayloads.clear();
        eventBus.replay("replay-topic", ReplayOptions.fromTimestamp(Instant.ofEpochMilli(now - 7000)));
        assertEquals(2, replayedPayloads.size());
        assertEquals("Msg 2", replayedPayloads.get(0).getContent());
        assertEquals("Msg 3", replayedPayloads.get(1).getContent());

        // 3. Replay from Offset (starting from offset 3)
        replayedPayloads.clear();
        eventBus.replay("replay-topic", ReplayOptions.fromOffset(3));
        assertEquals(1, replayedPayloads.size());
        assertEquals("Msg 3", replayedPayloads.get(0).getContent());
    }

    @Test
    public void testDeadLetterQueueRoutingAndManagement() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);

        // Subscribe a failing listener
        eventBus.subscribe("dlq-topic", DemoPayload.class, payload -> {
            latch.countDown();
            throw new RuntimeException("Simulated listener exception");
        });

        DemoPayload payload = new DemoPayload("Fail me", 100);
        eventBus.publish("dlq-topic", payload);

        // Wait for handler execution
        boolean finished = latch.await(2, TimeUnit.SECONDS);
        assertTrue(finished);

        // Yield execution to allow Virtual Thread to throw exception and hit the catch block writing to DLQ
        Thread.sleep(200);

        // Verify DLQ record is saved
        List<DeadLetterRecord> records = deadLetterStore.findAll();
        assertEquals(1, records.size());
        
        DeadLetterRecord record = records.get(0);
        assertEquals("Simulated listener exception", record.getErrorMessage());
        assertNotNull(record.getStackTrace());
        assertTrue(record.getStackTrace().contains("Simulated listener exception"));
        assertEquals("dlq-topic", record.getEvent().getTopic());
        
        DemoPayload deadPayload = serializer.deserialize(record.getEvent().getPayload(), DemoPayload.class);
        assertEquals("Fail me", deadPayload.getContent());

        // Delete from DLQ
        deadLetterStore.delete(record.getEvent().getId());
        List<DeadLetterRecord> recordsAfterDelete = deadLetterStore.findAll();
        assertTrue(recordsAfterDelete.isEmpty());
    }
}
