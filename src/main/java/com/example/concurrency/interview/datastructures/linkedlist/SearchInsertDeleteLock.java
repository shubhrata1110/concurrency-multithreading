package com.example.concurrency.interview.datastructures.linkedlist;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SearchInsertDeleteLock {
    private int activeSearchers=0;
    private boolean activeInserter=false;
    private boolean activeDeleter=false;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition okToSearch = lock.newCondition();
    private final Condition okToInsert = lock.newCondition();
    private final Condition okToDelete = lock.newCondition();


    public void startSearch() throws InterruptedException {
        lock.lockInterruptibly();
        try{
            while(activeDeleter)okToSearch.await();
            activeSearchers++;
        }
        finally {
            lock.unlock();
        }


    }
    public void stopSearch(){
        lock.lock();
        try{
            activeSearchers--;
            if(activeSearchers==0)okToDelete.signal();
        }
        finally {
            lock.unlock();
        }

    }
    public void startInsert() throws InterruptedException {
        lock.lockInterruptibly();
        try{
            while(activeDeleter || activeInserter)okToInsert.await();
            activeInserter=true;
        }
        finally {
            lock.unlock();
        }
    }

    public void stopInsert(){
        lock.lock();
        try{
            activeInserter=false;
            okToInsert.signal();
            okToDelete.signal();
        }
        finally {
            lock.unlock();
        }
    }

    public void startDelete() throws InterruptedException {
        lock.lockInterruptibly();
        try{
            while(activeSearchers>0 || activeInserter || activeDeleter)okToDelete.await();
            activeDeleter=true;
        }
        finally {

            lock.unlock();
        }


    }
    public void stopDelete(){
        lock.lock();
        try{
            activeDeleter=false;
            okToInsert.signal();
            okToSearch.signalAll();
            okToDelete.signal();          // FIX 3: the next deleter may go

        }
        finally {
            lock.unlock();

        }
    }


}
