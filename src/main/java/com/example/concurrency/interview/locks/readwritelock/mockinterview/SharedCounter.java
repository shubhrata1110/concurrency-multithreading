package com.example.concurrency.interview.locks.readwritelock.mockinterview;
public class SharedCounter {

    ReadWriteLock lock = new ReadWriteLock();
    private int counter=0;


    public int read() throws InterruptedException {
        lock.lockRead();
        try {
            return counter;
        }
        finally {
            lock.unlockRead();
        }
    }
    public void increment() throws InterruptedException {
        lock.lockWrite();
        try {
            counter++;
        }
        finally {
            lock.unlockWrite();
        }
    }
}
