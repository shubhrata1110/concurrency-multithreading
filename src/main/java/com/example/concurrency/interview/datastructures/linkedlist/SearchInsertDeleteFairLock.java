package com.example.concurrency.interview.datastructures.linkedlist;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

    public class SearchInsertDeleteFairLock {
        private int activeSearchers = 0;
        private boolean activeInserter = false;
        private boolean activeDeleter = false;
        private int waitingDeleters = 0;                 // NEW: deleters waiting to get in

        private final ReentrantLock lock = new ReentrantLock();
        private final Condition okToSearch = lock.newCondition();
        private final Condition okToInsert = lock.newCondition();
        private final Condition okToDelete = lock.newCondition();

        public void startSearch() throws InterruptedException {
            lock.lockInterruptibly();
            try {
                while (activeDeleter || waitingDeleters > 0) okToSearch.await();     // give way to waiting deleters
                activeSearchers++;
            } finally {
                lock.unlock();
            }
        }

        public void stopSearch() {
            lock.lock();
            try {
                activeSearchers--;
                if (activeSearchers == 0) okToDelete.signal();
            } finally {
                lock.unlock();
            }
        }

        public void startInsert() throws InterruptedException {
            lock.lockInterruptibly();
            try {
                while (activeDeleter || activeInserter || waitingDeleters > 0) okToInsert.await();
                activeInserter = true;
            } finally {
                lock.unlock();
            }
        }

        public void stopInsert() {
            lock.lock();
            try {
                activeInserter = false;
                if (waitingDeleters > 0) okToDelete.signal();    // a deleter is waiting: it goes next
                else okToInsert.signal();
            } finally {
                lock.unlock();
            }
        }

        public void startDelete() throws InterruptedException {
            lock.lockInterruptibly();
            try {
                waitingDeleters++;
                try {
                    while (activeSearchers > 0 || activeInserter || activeDeleter) okToDelete.await();
                } catch (InterruptedException e) {
                    waitingDeleters--;               // giving up: stop blocking everyone else
                    okToSearch.signalAll();
                    okToInsert.signal();
                    okToDelete.signal();
                    throw e;
                }
                waitingDeleters--;
                activeDeleter = true;
            } finally {
                lock.unlock();
            }
        }

        public void stopDelete() {
            lock.lock();
            try {
                activeDeleter = false;
                if (waitingDeleters > 0) {
                    okToDelete.signal();             // next deleter first
                } else {
                    okToSearch.signalAll();          // no deleters waiting: let everyone else in
                    okToInsert.signal();
                }
            } finally {
                lock.unlock();
            }
        }
    }
