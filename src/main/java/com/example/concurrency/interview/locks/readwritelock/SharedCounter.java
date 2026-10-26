package com.example.concurrency.interview.locks.readwritelock;
public class SharedCounter {
    private int count=0;
    ReadWriteLock lock = new ReadWriteLock();

    public int read() throws InterruptedException {
        lock.lockRead();
        try {
            return count;
        }
        finally {
            lock.unlockRead();
        }

    }
    public void write() throws InterruptedException {
        lock.lockWrite();
        try {
            count++;
        }
        finally {
            lock.unlockWrite();
        }
    }

}
