package com.example.concurrency.interview.locks.readwritelock.mockinterview;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReadWriteLock {
    private int activeReaders=0;
    private boolean isWriterActive=false;

    ReentrantLock lock = new ReentrantLock();
    Condition okToRead = lock.newCondition();
    Condition okToWrite = lock.newCondition();

    void lockRead() throws InterruptedException {
        lock.lockInterruptibly();
        try{
            while(isWriterActive)okToRead.await();
            activeReaders++;
        }
        finally {
            lock.unlock();
        }

    }
    void unlockRead()  {
        lock.lock();
        try{
            activeReaders--;
            if(activeReaders==0){
                okToWrite.signal();
            }

        }
        finally {
            lock.unlock();
        }

    }

    void lockWrite() throws InterruptedException {
        lock.lockInterruptibly();
        try{
           while(isWriterActive || activeReaders>0)okToWrite.await();
           isWriterActive=true;


        }
        finally {
            lock.unlock();
        }
    }

    void unlockWrite()  {
        lock.lock();
        try{
            isWriterActive=false;
            okToWrite.signal();
            okToRead.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

}
