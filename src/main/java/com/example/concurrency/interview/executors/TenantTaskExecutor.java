package com.example.concurrency.interview.executors;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TenantTaskExecutor {

    // a task + which tenant it belongs to
    private static class Job {
        String tenantId;
        Runnable task;

        Job(String tenantId, Runnable task) {
            this.tenantId = tenantId;
            this.task = task;
        }
    }

    private final Map<String, ArrayDeque<Runnable>> waiting = new HashMap<>();  // tenant → tasks not started yet
    private final Map<String, Integer> runningCount = new HashMap<>();          // tenant → tasks running now
    private boolean isShutdown = false;
    private final int maxPerTenant;
    private final List<Thread> workers = new ArrayList<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();

    public TenantTaskExecutor(int numWorkers, int maxPerTenant) {
        this.maxPerTenant = maxPerTenant;
        for (int i = 0; i < numWorkers; i++) {
            Thread worker = new Thread(this::workerLoop);
            workers.add(worker);
            worker.start();
        }
    }

    // add the task to its tenant's queue
    public void submit(String tenantId, Runnable task) {
        lock.lock();
        try {
            if (isShutdown) throw new IllegalStateException("shut down");
            waiting.computeIfAbsent(tenantId, k -> new ArrayDeque<>()).add(task);
            changed.signalAll();                     // a worker may be able to run it
        } finally {
            lock.unlock();
        }
    }

    // stop accepting tasks, finish all submitted ones, wait for workers to exit
    public void shutdown() throws InterruptedException {
        lock.lock();
        try {
            isShutdown = true;
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        for (Thread worker : workers) {
            worker.join();                           // outside the lock
        }
    }

    private void workerLoop() {
        while (true) {
            Job job;
            try {
                job = getTask();
            } catch (InterruptedException e) {
                return;
            }
            if (job == null) return;                 // shut down and nothing left

            try {
                job.task.run();                      // OUTSIDE the lock
            } catch (Exception e) {
                System.out.println(e);               // one bad task must not kill the worker
            } finally {
                finish(job.tenantId);                // always free the slot
            }
        }
    }

    // give the worker one task it is allowed to run now; wait if none
    private Job getTask() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (true) {
                for (String tenantId : waiting.keySet()) {
                    int count = runningCount.getOrDefault(tenantId, 0);
                    if (count < maxPerTenant) {                      // tenant has a free slot
                        ArrayDeque<Runnable> queue = waiting.get(tenantId);
                        Runnable task = queue.poll();                // its oldest task
                        if (queue.isEmpty()) waiting.remove(tenantId);
                        runningCount.put(tenantId, count + 1);
                        return new Job(tenantId, task);              // return right away (safe after remove)
                    }
                }
                if (isShutdown && waiting.isEmpty()) return null;    // no work left at all
                changed.await();                                     // wait for submit or finish
            }
        } finally {
            lock.unlock();
        }
    }

    // a task ended: its tenant has one free slot
    private void finish(String tenantId) {
        lock.lock();
        try {
            runningCount.put(tenantId, runningCount.get(tenantId) - 1);
            changed.signalAll();                     // waiting workers check again
        } finally {
            lock.unlock();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        TenantTaskExecutor ex = new TenantTaskExecutor(2, 1);   // 2 workers, max 1 per tenant
        long start = System.currentTimeMillis();

        String[] names = {"A1", "A2", "B1"};
        for (String name : names) {
            String tenant = name.substring(0, 1);                // "A1" → "A"
            ex.submit(tenant, () -> {
                System.out.println(name + " starts at " + (System.currentTimeMillis() - start) + " ms");
                try {
                    Thread.sleep(500);                           // pretend backup work
                } catch (InterruptedException e) { }
            });
        }

        ex.shutdown();
        System.out.println("all done at " + (System.currentTimeMillis() - start) + " ms");
    }
}