package org.flowbridge.core.domain.model;

import java.util.HashMap;
import java.util.Map;

public class Event {
    private String id;
    private String topic;
    private byte[] payload;
    private String payloadType;
    private long timestamp;
    private long offset;
    private Map<String, String> headers = new HashMap<>();

    public Event() {
    }

    public Event(String id, String topic, byte[] payload, String payloadType, long timestamp, long offset, Map<String, String> headers) {
        this.id = id;
        this.topic = topic;
        this.payload = payload;
        this.payloadType = payloadType;
        this.timestamp = timestamp;
        this.offset = offset;
        this.headers = headers != null ? headers : new HashMap<>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public String getPayloadType() {
        return payloadType;
    }

    public void setPayloadType(String payloadType) {
        this.payloadType = payloadType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers != null ? headers : new HashMap<>();
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", topic='" + topic + '\'' +
                ", payloadType='" + payloadType + '\'' +
                ", timestamp=" + timestamp +
                ", offset=" + offset +
                ", headers=" + headers +
                '}';
    }
}
