package com.example.concurrency.interview.resourcesharing;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

interface Connection { }
interface ConnectionFactory { Connection create(); }

public class ConnectionPool {
    private final Queue<Connection> free = new ArrayDeque<>();   // connections not borrowed right now
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition available = lock.newCondition();

    public ConnectionPool(int size, ConnectionFactory factory) {
        if (size <= 0) throw new IllegalArgumentException("size must be > 0");
        for (int i = 0; i < size; i++) {
            free.add(factory.create());          // the ONLY place create() is called
        }
    }

    // borrow: wait while none is free
    public Connection getConnection(long timeout, TimeUnit unit) throws InterruptedException {
        long timeOutAt= System.currentTimeMillis()+unit.toMillis(timeout);
        lock.lockInterruptibly();
        try {
            while (free.isEmpty()) {
                long timeLeft = timeOutAt - System.currentTimeMillis();
                if(timeLeft <= 0) {return null;}
                available.await(timeLeft, TimeUnit.MILLISECONDS);
            }
            return free.poll();
        } finally {
            lock.unlock();
        }
    }

    // give back: put it in the queue, wake ONE waiting thread
    public void releaseConnection(Connection conn) {
        if (conn == null) throw new NullPointerException("conn is null");
        lock.lock();
        try {
            free.add(conn);
            available.signal();                  // one connection freed → one waiter can use it
        } finally {
            lock.unlock();
        }
    }
}

