package org.flowbridge.embedded;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class RocksDBManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RocksDBManager.class);

    static {
        RocksDB.loadLibrary();
    }

    private final RocksDB db;
    private final Options options;
    private final String dbPath;

    public RocksDBManager(String dbPath) {
        this.dbPath = dbPath;
        this.options = new Options().setCreateIfMissing(true);
        try {
            File dir = new File(dbPath);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            this.db = RocksDB.open(options, dbPath);
            log.info("RocksDB successfully opened at path: {}", dbPath);
        } catch (RocksDBException e) {
            log.error("Failed to open RocksDB at path: {}", dbPath, e);
            throw new RuntimeException("Failed to initialize RocksDB", e);
        }
    }

    public RocksDB getDb() {
        return db;
    }

    @Override
    public void close() {
        log.info("Closing RocksDB at path: {}", dbPath);
        if (db != null) {
            db.close();
        }
        if (options != null) {
            options.close();
        }
    }
}
