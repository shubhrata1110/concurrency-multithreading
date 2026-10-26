package com.example.concurrency.interview.resourcesharing.ratelimiter;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class TokenBucketRateLimiter {
    private final int capacity;             // max tokens the bucket can hold (max burst)
    private final double tokensPerMs;       // refill speed
    private double tokens;                  // tokens available now
    private long lastRefillTime;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition tokenMaybeAvailable = lock.newCondition();   // only used by acquire()

    public TokenBucketRateLimiter(int capacity, double tokensPerSecond) {
        this.capacity = capacity;
        this.tokensPerMs = tokensPerSecond / 1000.0;
        this.tokens = capacity;             // start full
        this.lastRefillTime = System.currentTimeMillis();
    }

    // NON-BLOCKING: take a token if there is one, otherwise return false immediately.
    public boolean tryAcquire() {
        lock.lock();
        try {
            refill();
            if (tokens >= 1) {
                tokens -= 1;
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    // BLOCKING: wait until a token is available, then take it.
    public void acquire() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (true) {
                refill();
                if (tokens >= 1) {
                    tokens -= 1;
                    return;
                }
                // how long until the bucket has 1 whole token
                long waitMs = (long) Math.ceil((1 - tokens) / tokensPerMs);
                tokenMaybeAvailable.await(Math.max(1, waitMs), TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    // Caller holds the lock. Add the tokens earned since the last refill.
    private void refill() {
        long now = System.currentTimeMillis();
        tokens = Math.min(capacity, tokens + (now - lastRefillTime) * tokensPerMs);
        lastRefillTime = now;
    }
}