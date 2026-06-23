package org.flowbridge.core.domain.model;

import java.time.Instant;

public record ReplayOptions(
    ReplayType type,
    Instant timestamp,
    Long offset
) {
    public enum ReplayType {
        ALL,
        FROM_TIMESTAMP,
        FROM_OFFSET
    }

    public static ReplayOptions all() {
        return new ReplayOptions(ReplayType.ALL, null, null);
    }

    public static ReplayOptions fromTimestamp(Instant timestamp) {
        return new ReplayOptions(ReplayType.FROM_TIMESTAMP, timestamp, null);
    }

    public static ReplayOptions fromOffset(long offset) {
        return new ReplayOptions(ReplayType.FROM_OFFSET, null, offset);
    }
}
