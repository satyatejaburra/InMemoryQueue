package com.phonepe.api;

import java.util.concurrent.TimeUnit;

public class Record {

    private static final long DEFAULT_TTL = 60000;

    private Message message;
    private long timeStamp;
    private long ttl;

    public Record(Message message) {
        this.message = message;
        this.ttl = 60000;
        this.timeStamp = System.currentTimeMillis();
    }

    public Record(Message message, long ttl, TimeUnit timeUnit) {
        this.message = message;
        processTTL(ttl, timeUnit);
        this.ttl = ttl;
        this.timeStamp = System.currentTimeMillis();
    }

    private void processTTL(long ttl, TimeUnit timeUnit) {
        switch (timeUnit) {
            case DAYS:
                this.ttl = ttl * 86400000;
                break;

            case HOURS:
                this.ttl = ttl * 3600000;
                break;
            case MINUTES:
                this.ttl = ttl * 60000;
                break;
            case SECONDS:
                this.ttl = ttl * 1000;
            case MILLISECONDS:
                this.ttl = ttl;
                break;
            default:
                this.ttl = DEFAULT_TTL;
        }
    }

    public Message getMessage() {
        return message;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public long getTtl() {
        return ttl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record)) return false;

        Record record = (Record) o;

        if (getTimeStamp() != record.getTimeStamp()) return false;
        if (getTtl() != record.getTtl()) return false;
        return getMessage() != null ? getMessage().equals(record.getMessage()) : record.getMessage() == null;
    }

    @Override
    public int hashCode() {
        int result = getMessage() != null ? getMessage().hashCode() : 0;
        result = 31 * result + (int) (getTimeStamp() ^ (getTimeStamp() >>> 32));
        result = 31 * result + (int) (getTtl() ^ (getTtl() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Record{" +
                "message=" + message +
                ", timeStamp=" + timeStamp +
                ", ttl=" + ttl +
                '}';
    }

    public boolean isMessageExpired() {
        long currentTimeStamp = System.currentTimeMillis();
        return currentTimeStamp - (timeStamp + ttl) > 0;
    }
}

