package com.example.concurrency.interview.scheduling.delayeddispatcher;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DelayedDispatcherSync {

    private static class Job {
        private final Runnable task;
        private final long runAt;

        Job(Runnable task, long runAt) {
            this.task = task;
            this.runAt = runAt;
        }
    }

    private final PriorityQueue<Job> queue = new PriorityQueue<>((a, b) -> Long.compare(a.runAt, b.runAt));
    private boolean isStopped = false;
    private final ExecutorService executorService;
    private final Thread dispatcher;

    public DelayedDispatcherSync(int numWorkers) {
        executorService = Executors.newFixedThreadPool(numWorkers);
        dispatcher = new Thread(this::dispatcherLoop, "dispatcher");
        dispatcher.start();
    }

    // synchronized method = lock.lock() ... finally unlock()
    public synchronized void schedule(Runnable task, long delayMs) {
        if (task == null) throw new NullPointerException("task is null");
        if (isStopped) throw new IllegalStateException("dispatcher is shut down");
        queue.add(new Job(task, System.currentTimeMillis() + delayMs));
        notifyAll();                         // = changed.signal(): a new, maybe earlier, job
    }

    // NOT synchronized: we must not hold the lock while joining
    public void shutdown() throws InterruptedException {
        synchronized (this) {
            isStopped = true;
            notifyAll();                     // wake the dispatcher so it sees isStopped
        }                                    // lock released here
        dispatcher.join();
        executorService.shutdown();
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

    private synchronized Job getJob() throws InterruptedException {
        while (true) {
            if (isStopped) return null;
            if (queue.isEmpty()) {
                wait();                      // = changed.await()
                continue;
            }
            long waitMs = queue.peek().runAt - System.currentTimeMillis();
            if (waitMs <= 0) return queue.poll();
            wait(waitMs);                    // = changed.await(waitMs, MILLISECONDS)
        }
    }
}