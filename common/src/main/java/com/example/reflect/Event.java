package com.example.reflect;

import com.google.common.base.MoreObjects;

/**
 * Message value
 */
public class Event {

    private String timestamp;
    private String message;

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("timestamp", timestamp)
                .add("message", message)
                .toString();
    }
}
