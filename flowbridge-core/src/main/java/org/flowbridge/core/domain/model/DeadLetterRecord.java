package org.flowbridge.core.domain.model;

public class DeadLetterRecord {
    private Event event;
    private String errorMessage;
    private String stackTrace;
    private long failureTimestamp;

    public DeadLetterRecord() {
    }

    public DeadLetterRecord(Event event, String errorMessage, String stackTrace, long failureTimestamp) {
        this.event = event;
        this.errorMessage = errorMessage;
        this.stackTrace = stackTrace;
        this.failureTimestamp = failureTimestamp;
    }

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }

    public long getFailureTimestamp() {
        return failureTimestamp;
    }

    public void setFailureTimestamp(long failureTimestamp) {
        this.failureTimestamp = failureTimestamp;
    }

    @Override
    public String toString() {
        return "DeadLetterRecord{" +
                "event=" + event +
                ", errorMessage='" + errorMessage + '\'' +
                ", failureTimestamp=" + failureTimestamp +
                '}';
    }
}
