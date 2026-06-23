package org.flowbridge.embedded;

import org.flowbridge.core.application.port.out.ReplayableStore;
import org.flowbridge.core.domain.model.Event;
import org.flowbridge.core.infrastructure.serialization.Serializer;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RocksDBReplayableStore implements ReplayableStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBReplayableStore.class);

    private final RocksDBManager dbManager;
    private final Serializer serializer;
    private final Map<String, Object> locks = new ConcurrentHashMap<>();

    public RocksDBReplayableStore(RocksDBManager dbManager, Serializer serializer) {
        this.dbManager = dbManager;
        this.serializer = serializer;
    }

    @Override
    public void save(Event event) {
        try {
            RocksDB db = dbManager.getDb();
            long offset = getNextOffset(event.getTopic(), db);
            event.setOffset(offset);

            byte[] key = createEventKey(event.getTopic(), offset);
            byte[] value = serializer.serialize(event);
            db.put(key, value);
            log.debug("Saved event to RocksDB: topic={}, offset={}", event.getTopic(), offset);
        } catch (RocksDBException e) {
            log.error("Failed to save event to RocksDB", e);
            throw new RuntimeException("Database error during save", e);
        }
    }

    @Override
    public List<Event> findByTopic(String topic) {
        List<Event> events = new ArrayList<>();
        byte[] prefix = createTopicPrefix(topic);
        RocksDB db = dbManager.getDb();

        try (RocksIterator iter = db.newIterator()) {
            iter.seek(prefix);
            while (iter.isValid() && startsWith(iter.key(), prefix)) {
                Event event = serializer.deserialize(iter.value(), Event.class);
                events.add(event);
                iter.next();
            }
        }
        return events;
    }

    @Override
    public List<Event> findByTopicFromTimestamp(String topic, Instant timestamp) {
        List<Event> allEvents = findByTopic(topic);
        long epochMilli = timestamp.toEpochMilli();
        return allEvents.stream()
                .filter(e -> e.getTimestamp() >= epochMilli)
                .toList();
    }

    @Override
    public List<Event> findByTopicFromOffset(String topic, long offset) {
        List<Event> events = new ArrayList<>();
        byte[] prefix = createTopicPrefix(topic);
        byte[] startKey = createEventKey(topic, offset);
        RocksDB db = dbManager.getDb();

        try (RocksIterator iter = db.newIterator()) {
            iter.seek(startKey);
            while (iter.isValid() && startsWith(iter.key(), prefix)) {
                Event event = serializer.deserialize(iter.value(), Event.class);
                events.add(event);
                iter.next();
            }
        }
        return events;
    }

    public List<String> listTopics() {
        List<String> topics = new ArrayList<>();
        byte[] prefix = "events:".getBytes(StandardCharsets.UTF_8);
        RocksDB db = dbManager.getDb();
        try (RocksIterator iter = db.newIterator()) {
            iter.seek(prefix);
            String lastTopic = null;
            while (iter.isValid() && startsWith(iter.key(), prefix)) {
                String keyStr = new String(iter.key(), StandardCharsets.UTF_8);
                String[] parts = keyStr.split(":");
                if (parts.length >= 3) {
                    String topic = parts[1];
                    if (!topic.equals(lastTopic)) {
                        topics.add(topic);
                        lastTopic = topic;
                    }
                }
                iter.next();
            }
        }
        return topics;
    }

    private long getNextOffset(String topic, RocksDB db) throws RocksDBException {
        synchronized (locks.computeIfAbsent(topic, k -> new Object())) {
            byte[] key = ("offsets:" + topic).getBytes(StandardCharsets.UTF_8);
            byte[] value = db.get(key);
            long nextOffset = 1;
            if (value != null) {
                nextOffset = ByteBuffer.wrap(value).getLong() + 1;
            }
            byte[] nextValue = ByteBuffer.allocate(8).putLong(nextOffset).array();
            db.put(key, nextValue);
            return nextOffset;
        }
    }

    private byte[] createTopicPrefix(String topic) {
        return ("events:" + topic + ":").getBytes(StandardCharsets.UTF_8);
    }

    private byte[] createEventKey(String topic, long offset) {
        byte[] prefix = createTopicPrefix(topic);
        return ByteBuffer.allocate(prefix.length + 8)
                .put(prefix)
                .putLong(offset)
                .array();
    }

    private boolean startsWith(byte[] array, byte[] prefix) {
        if (array.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (array[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }
}
