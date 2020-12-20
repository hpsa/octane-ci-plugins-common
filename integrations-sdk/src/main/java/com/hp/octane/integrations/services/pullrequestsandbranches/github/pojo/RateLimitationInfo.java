package com.hp.octane.integrations.services.pullrequestsandbranches.github.pojo;

public class RateLimitationInfo {

    private int limit;
    private int remaining;
    private int used;
    private long reset;

    public int getLimit() {
        return limit;
    }

    public RateLimitationInfo setLimit(int limit) {
        this.limit = limit;
        return this;
    }

    public int getRemaining() {
        return remaining;
    }

    public RateLimitationInfo setRemaining(int remaining) {
        this.remaining = remaining;
        return this;
    }

    public int getUsed() {
        return used;
    }

    public RateLimitationInfo setUsed(int used) {
        this.used = used;
        return this;
    }

    public long getReset() {
        return reset;
    }

    public RateLimitationInfo setReset(long reset) {
        this.reset = reset;
        return this;
    }
}
