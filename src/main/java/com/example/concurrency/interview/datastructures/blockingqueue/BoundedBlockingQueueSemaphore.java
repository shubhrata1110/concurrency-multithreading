package com.example.concurrency.interview.datastructures.blockingqueue;
import java.util.concurrent.Semaphore;

public class BoundedBlockingQueueSemaphore<T> {
    private final Object[] items;
    private int head = 0, tail = 0;

    private final Semaphore freeSlots;                 // how many more items fit
    private final Semaphore filledSlots;               // how many items are inside
    private final Semaphore mutex = new Semaphore(1);  // one thread at a time touches the array

    public BoundedBlockingQueueSemaphore(int capacity) {
        items = new Object[capacity];
        freeSlots = new Semaphore(capacity);           // all slots free at the start
        filledSlots = new Semaphore(0);                // nothing to take at the start
    }

    public void put(T item) throws InterruptedException {
        freeSlots.acquire();                           // 1. wait for a free slot (BEFORE the mutex)
        mutex.acquire();                               // 2. lock the array
        try {
            items[tail] = item;
            tail = (tail + 1) % items.length;
        } finally {
            mutex.release();
        }
        filledSlots.release();                         // 3. one more item for consumers
    }

    @SuppressWarnings("unchecked")
    public T take() throws InterruptedException {
        filledSlots.acquire();                         // 1. wait for an item (BEFORE the mutex)
        T item;
        mutex.acquire();                               // 2. lock the array
        try {
            item = (T) items[head];
            items[head] = null;
            head = (head + 1) % items.length;
        } finally {
            mutex.release();
        }
        freeSlots.release();                           // 3. one more free slot for producers
        return item;
    }
}