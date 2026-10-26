package com.example.concurrency.interview.executors;
import java.util.*;
import java.util.concurrent.locks.*;
public class WorkerPool {
    private final Queue<Runnable> tasks = new ArrayDeque<>();    // submitted, not started yet
    private int pending = 0;                                       // submitted but not finished
    private boolean shutdown = false;
    private final List<Exception> failures = new ArrayList<>();   // tasks that threw
    private final List<Thread> workers = new ArrayList<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();        // "something changed, re-check"

    public WorkerPool(int numWorkers) {
        for (int i = 0; i < numWorkers; i++) {
            Thread t = new Thread(this::workerLoop);
            workers.add(t);
            t.start();
        }
    }

    // ---------------- client API ----------------

    // Accept the task and return immediately (does NOT run it here).
    public void submitWork(Runnable task) {
        lock.lock();
        try {
            if (shutdown) throw new IllegalStateException("pool is shut down");
            tasks.add(task);
            pending++;
            changed.signalAll();             // wake idle workers
        } finally {
            lock.unlock();
        }
    }

    // Wait until every task submitted so far has finished.
    public void blockUntilAllDone() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (pending > 0) {
                changed.await();
            }
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() throws InterruptedException {
        lock.lock();
        try {
            shutdown = true;
            changed.signalAll();             // wake idle workers so they can exit
        } finally {
            lock.unlock();
        }
        for (Thread t : workers) t.join();
    }

    public List<Exception> getFailures() {
        lock.lock();
        try {
            return new ArrayList<>(failures);
        } finally {
            lock.unlock();
        }
    }

    // ---------------- worker side (same shape as the scheduler) ----------------

    private void workerLoop() {
        while (true) {
            Runnable task;
            try {
                task = take();
            } catch (InterruptedException e) {
                return;
            }
            if (task == null) return;            // shut down and nothing left

            try {
                task.run();                      // run OUTSIDE the lock
                finish(null);
            } catch (Exception e) {
                finish(e);
            }
        }
    }

    // Give a worker one task. Wait while there is nothing to do (and not shut down).
    private Runnable take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (tasks.isEmpty() && !shutdown) {
                changed.await();
            }
            if (tasks.isEmpty()) return null;    // shut down and nothing left
            return tasks.poll();
        } finally {
            lock.unlock();
        }
    }

    // Called when a task ends, successfully (e == null) or not.
    private void finish(Exception e) {
        lock.lock();
        try {
            pending--;
            if (e != null) failures.add(e);
            if (pending == 0) changed.signalAll();   // wake blockUntilAllDone
        } finally {
            lock.unlock();
        }
    }
}