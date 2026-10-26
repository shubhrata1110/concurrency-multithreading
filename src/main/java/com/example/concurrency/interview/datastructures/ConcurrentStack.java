package com.example.concurrency.interview.datastructures;
import java.util.concurrent.locks.*;

public class ConcurrentStack<T> {
    private class Node {
        final T value;
        Node next;
        Node(T value, Node next) { this.value = value; this.next = next; }
    }

    private Node head = null;                // top of the stack
    private int size = 0;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    public void push(T value) {
        lock.lock();
        try {
            head = new Node(value, head);    // new node points to the old top
            size++;
            notEmpty.signal();               // a waiting pop can proceed
        } finally {
            lock.unlock();
        }
    }

    // Non-blocking: returns null if empty.
    public T pop() {
        lock.lock();
        try {
            if (head == null) return null;
            T value = head.value;
            head = head.next;                // second node becomes the top
            size--;
            return value;
        } finally {
            lock.unlock();
        }
    }

    // Blocking: waits while empty.
    public T popBlocking() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (head == null) {
                notEmpty.await();
            }
            T value = head.value;
            head = head.next;
            size--;
            return value;
        } finally {
            lock.unlock();
        }
    }

    public T peek() {
        lock.lock();
        try {
            return head == null ? null : head.value;
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return size;
        } finally {
            lock.unlock();
        }
    }
}