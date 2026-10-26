package com.example.concurrency.interview.scheduling.delayeddispatcher;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DelayedDispatcher {

    private static class Job {
        private final Runnable task;
        private final long runAt;

        public Job(Runnable task, long runAt) {
            this.task = task;
            this.runAt = runAt;
        }
    }

    private final PriorityQueue<Job> queue = new PriorityQueue<>((a, b) -> Long.compare(a.runAt, b.runAt));
    private boolean isStopped = false;
    private final ExecutorService executorService;
    private final Thread dispatcher;                       // kept as a field so shutdown() can join it

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();

    public DelayedDispatcher(int numWorkers) {
        executorService = Executors.newFixedThreadPool(numWorkers);
        dispatcher = new Thread(this::dispatcherLoop, "dispatcher");
        dispatcher.start();
    }

    public void schedule(Runnable task, long delayMs) {
        if (task == null) throw new NullPointerException("task is null");
        lock.lock();
        try {
            if (isStopped) throw new IllegalStateException("dispatcher is shut down");
            long runAt = System.currentTimeMillis() + delayMs;
            queue.add(new Job(task, runAt));
            changed.signal();
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() throws InterruptedException {
        lock.lock();
        try {
            isStopped = true;
            changed.signal();                // wake the dispatcher so it sees isStopped
        } finally {
            lock.unlock();
        }
        dispatcher.join();                   // dispatcher exits
        executorService.shutdown();       // workers finish what they were given, then exit
        executorService.awaitTermination(5, TimeUnit.SECONDS);
    }

    private void dispatcherLoop() {
        while (true) {
            Job job;
            try {
                job = getJob();
            } catch (InterruptedException e) {
                return;
            }
            if (job == null) return;         // stopped
            executorService.execute(job.task);   // outside the lock
        }
    }

    private Job getJob() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (true) {
                if (isStopped) return null;
                if (queue.isEmpty()) {
                    changed.await();         // nothing at all: wait for schedule()
                    continue;                // re-check from the top (stopped? still empty?)
                }
                long waitMs = queue.peek().runAt - System.currentTimeMillis();
                if (waitMs <= 0) return queue.poll();
                changed.await(waitMs, TimeUnit.MILLISECONDS);
            }
        } finally {
            lock.unlock();
        }
    }
}