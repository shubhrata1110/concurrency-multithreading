package com.example.concurrency.interview.datastructures.blockingqueue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BlockingQueueWithTimeout<T> {
    private final Object[] items;      // fixed-size circular buffer
    private int head = 0;              // next index to take from
    private int tail = 0;              // next index to put into
    private int count = 0;             // items currently in the queue

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();   // producers wait here
    private final Condition notEmpty = lock.newCondition();  // consumers wait here

    public BlockingQueueWithTimeout(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        items = new Object[capacity];
    }

    // Add an item. Waits while the queue is full.
    public void put(T item) throws InterruptedException {
        if (item == null) throw new NullPointerException("null items not allowed");
        lock.lockInterruptibly();
        try {
            while (count == items.length) {
                notFull.await();
            }
            items[tail] = item;
            tail = (tail + 1) % items.length;
            count++;
            notEmpty.signal();                          // wake one waiting consumer
        } finally {
            lock.unlock();
        }
    }

    // Remove the oldest item. Waits as long as needed while the queue is empty.
    @SuppressWarnings("unchecked")
    public T take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (count == 0) {
                notEmpty.await();
            }
            T item = (T) items[head];
            items[head] = null;
            head = (head + 1) % items.length;
            count--;
            notFull.signal();                           // wake one waiting producer
            return item;
        } finally {
            lock.unlock();
        }
    }

    // Remove the oldest item. Waits at most `timeout`; returns null if nothing arrived in time.
    @SuppressWarnings("unchecked")
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanosLeft = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (count == 0) {
                if (nanosLeft <= 0) {
                    return null;                        // time's up and still empty
                }
                nanosLeft = notEmpty.awaitNanos(nanosLeft);
            }
            T item = (T) items[head];
            items[head] = null;
            head = (head + 1) % items.length;
            count--;
            notFull.signal();
            return item;
        } finally {
            lock.unlock();
        }
    }

    // Remove and return everything currently in the queue, without waiting.
    @SuppressWarnings("unchecked")
    public List<T> drain() {
        lock.lock();
        try {
            List<T> all = new ArrayList<>(count);
            while (count > 0) {
                T item = (T) items[head];
                items[head] = null;
                head = (head + 1) % items.length;
                count--;
                all.add(item);
            }
            notFull.signalAll();                        // every slot is free: wake all waiting producers
            return all;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return count;
        } finally {
            lock.unlock();
        }
    }
}