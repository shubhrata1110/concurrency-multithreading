package com.example.concurrency.interview.datastructures.blockingqueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BoundedBlockingQueue<T> {
    private final Object[] items;      // fixed-size circular buffer
    private int head = 0;              // next index to take from
    private int tail = 0;              // next index to put into
    private int count = 0;             // items currently in the queue

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();   // producers wait here
    private final Condition notEmpty = lock.newCondition();  // consumers wait here

    public BoundedBlockingQueue(int capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        items = new Object[capacity];
    }

    // Blocks while full. Passes InterruptedException up to the caller.
    public void put(T item) throws InterruptedException {
        if (item == null) throw new NullPointerException("null items not allowed");
        lock.lockInterruptibly();          // before try: if this throws, we don't hold the lock
        try {
            while (count == items.length) {
                notFull.await();           // releases the lock while waiting
            }
            items[tail] = item;
            tail = (tail + 1) % items.length;
            count++;
            notEmpty.signal();             // wake one waiting consumer
        } finally {
            lock.unlock();
        }
    }

    // Blocks while empty. Passes InterruptedException up to the caller.
    @SuppressWarnings("unchecked")
    public T take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (count == 0) {
                notEmpty.await();
            }
            T item = (T) items[head];
            items[head] = null;            // let the garbage collector reclaim it
            head = (head + 1) % items.length;
            count--;
            notFull.signal();              // wake one waiting producer
            return item;
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