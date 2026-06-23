package org.flowbridge.embedded;

import org.flowbridge.core.application.port.out.DeadLetterStore;
import org.flowbridge.core.domain.model.DeadLetterRecord;
import org.flowbridge.core.domain.model.Event;
import org.flowbridge.core.infrastructure.serialization.Serializer;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RocksDBDeadLetterStore implements DeadLetterStore {
    private static final Logger log = LoggerFactory.getLogger(RocksDBDeadLetterStore.class);

    private final RocksDBManager dbManager;
    private final Serializer serializer;

    public RocksDBDeadLetterStore(RocksDBManager dbManager, Serializer serializer) {
        this.dbManager = dbManager;
        this.serializer = serializer;
    }

    @Override
    public void saveDeadLetter(Event event, Throwable cause) {
        try {
            RocksDB db = dbManager.getDb();
            byte[] key = createDlqKey(event.getId());

            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            cause.printStackTrace(pw);
            String stackTrace = sw.toString();

            DeadLetterRecord record = new DeadLetterRecord(
                    event,
                    cause.getMessage() != null ? cause.getMessage() : cause.getClass().getName(),
                    stackTrace,
                    System.currentTimeMillis()
            );

            byte[] value = serializer.serialize(record);
            db.put(key, value);
            log.info("Saved dead letter record for event {} to RocksDB", event.getId());
        } catch (RocksDBException e) {
            log.error("Failed to save dead letter record to RocksDB", e);
            throw new RuntimeException("Database error during DLQ save", e);
        }
    }

    @Override
    public List<DeadLetterRecord> findAll() {
        List<DeadLetterRecord> records = new ArrayList<>();
        byte[] prefix = "dlq:".getBytes(StandardCharsets.UTF_8);
        RocksDB db = dbManager.getDb();

        try (RocksIterator iter = db.newIterator()) {
            iter.seek(prefix);
            while (iter.isValid() && startsWith(iter.key(), prefix)) {
                DeadLetterRecord record = serializer.deserialize(iter.value(), DeadLetterRecord.class);
                records.add(record);
                iter.next();
            }
        }
        return records;
    }

    @Override
    public void delete(String eventId) {
        try {
            RocksDB db = dbManager.getDb();
            byte[] key = createDlqKey(eventId);
            db.delete(key);
            log.debug("Deleted dead letter record for event {}", eventId);
        } catch (RocksDBException e) {
            log.error("Failed to delete dead letter record from RocksDB", e);
            throw new RuntimeException("Database error during DLQ delete", e);
        }
    }

    private byte[] createDlqKey(String eventId) {
        return ("dlq:" + eventId).getBytes(StandardCharsets.UTF_8);
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
