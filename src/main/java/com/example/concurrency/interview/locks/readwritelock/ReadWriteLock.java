package com.example.concurrency.interview.locks.readwritelock;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ReadWriteLock {
    private int activeReaders=0;
    private boolean writerActive =false;

    private final ReentrantLock lock =new ReentrantLock();
    private final Condition okToRead=lock.newCondition();
    private final Condition okToWrite=lock.newCondition();

    public void lockRead() throws InterruptedException {
        lock.lockInterruptibly();
        try{
            while(writerActive)okToRead.await();
            activeReaders++;

        }
        finally{

            lock.unlock();
        }

    }
    public void unlockRead() throws InterruptedException {
        lock.lock();
        try{
            activeReaders--;
            if(activeReaders==0){
                okToWrite.signal();

            }
        }
        finally{
            lock.unlock();

        }
    }
    public void lockWrite() throws InterruptedException {
        lock.lockInterruptibly();
        try{
            while(writerActive || activeReaders>0)okToWrite.await();
            writerActive=true;

        }
        finally{
            lock.unlock();
        }
    }
    public void unlockWrite() throws InterruptedException {
        lock.lock();
        try{
            writerActive=false;
            okToWrite.signal();
            okToRead.signalAll();
        }
        finally{
            lock.unlock();
        }
    }

}
