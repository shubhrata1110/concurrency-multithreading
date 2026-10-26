package com.example.concurrency.interview.locks.readwritelock;
public class ReadWriteLockSync {
    private int activeReaders = 0;
    private boolean writerActive = false;

    public synchronized void lockRead() throws InterruptedException {

        while (writerActive) {
            wait();                      // wait while a writer is inside
        }
        activeReaders++;
    }

    public synchronized void unlockRead() {
        activeReaders--;
        if (activeReaders == 0) {
            notifyAll();                 // last reader out: a writer may enter now
        }
    }

    public synchronized void lockWrite() throws InterruptedException {
        while (writerActive || activeReaders > 0) {
            wait();                      // wait while anyone is inside
        }
        writerActive = true;
    }

    public synchronized void unlockWrite() {
        writerActive = false;
        notifyAll();                     // wake everyone: all readers, or one writer, can go
    }
}
