package com.example.concurrency.interview.resourcesharing.ratelimiter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SlidingWindowRateLimiter {
    private final int capacity;
    private final long windowSize;
    private final Queue<Long>expiryTimes=new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition slotMaybeFree = lock.newCondition();

    public SlidingWindowRateLimiter(int capacity, long windowSize) {
        this.capacity = capacity;
        this.windowSize = windowSize;

    }
    //non blocking
    // non-blocking: yes or no right away
    public boolean tryAcquire() {
        lock.lock();
        try {
            long now = System.currentTimeMillis();
            removeExpired(now);
            if (expiryTimes.size() < capacity) {
                expiryTimes.add(now + windowSize);     // take a ticket
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    // blocking: wait until a ticket is free
    public void acquire() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (true) {
                long now = System.currentTimeMillis();
                removeExpired(now);
                if (expiryTimes.size() < capacity) {
                    expiryTimes.add(now + windowSize);
                    return;
                }
                long waitMs = expiryTimes.peek() - now;           // until the oldest ticket expires
                slotMaybeFree.await(waitMs, TimeUnit.MILLISECONDS);
            }                                                     // woke up: check again
        } finally {
            lock.unlock();
        }
    }

    private void removeExpired(long now) {
        while (!expiryTimes.isEmpty() && expiryTimes.peek() <= now) {
            expiryTimes.poll();
        }
    }
}

