package com.example.concurrency.interview.datastructures.blockingqueue;
public class BoundedBlockingQueueSync<T> {
    private final Object[] items;
    private int head = 0, tail = 0, count = 0;

    public BoundedBlockingQueueSync(int capacity) {
        items = new Object[capacity];
    }

    public synchronized void put(T item) throws InterruptedException {
        while (count == items.length) {
            wait();                                    // releases the lock while waiting
        }
        items[tail] = item;
        tail = (tail + 1) % items.length;
        count++;
        notifyAll();                                   // ONE wait set: must wake everyone
    }

    @SuppressWarnings("unchecked")
    public synchronized T take() throws InterruptedException {
        while (count == 0) {
            wait();
        }
        T item = (T) items[head];
        items[head] = null;
        head = (head + 1) % items.length;
        count--;
        notifyAll();
        return item;
    }
}
