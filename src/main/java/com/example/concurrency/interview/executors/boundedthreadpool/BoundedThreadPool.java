package com.example.concurrency.interview.executors.boundedthreadpool;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.*;

public class BoundedThreadPool {
    private final Queue<Runnable> tasks = new ArrayDeque<>();   // submitted, not started yet
    private final int capacity;                                  // max tasks waiting in the queue
    private boolean shutdown = false;
    private final List<Thread> workers = new ArrayList<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();       // "something changed, re-check"

    public BoundedThreadPool(int numWorkers, int capacity) {
        this.capacity = capacity;
        for (int i = 0; i < numWorkers; i++) {
            Thread t = new Thread(this::workerLoop, "worker-" + i);
            workers.add(t);
            t.start();
        }
    }

    // ---------------- client API ----------------

    // Add a task. Waits while the queue is full. Refused after shutdown.
    public void execute(Runnable task) throws InterruptedException {
        if (task == null) throw new NullPointerException("task is null");
        lock.lockInterruptibly();
        try {
            while (tasks.size() == capacity && !shutdown) {
                changed.await();
            }
            if (shutdown) throw new IllegalStateException("pool is shut down");
            tasks.add(task);
            changed.signalAll();                 // wake an idle worker
        } finally {
            lock.unlock();
        }
    }

    // No new tasks; workers finish everything already queued, then stop.
    public void shutdown() {
        lock.lock();
        try {
            shutdown = true;
            changed.signalAll();                 // idle workers wake up, see shutdown + empty queue, exit
        } finally {
            lock.unlock();
        }
    }

    // No new tasks; remove queued tasks and return them; interrupt workers so they stop now.
    public List<Runnable> shutdownNow() {
        List<Runnable> notStarted = new ArrayList<>();
        lock.lock();
        try {
            shutdown = true;
            while (!tasks.isEmpty()) notStarted.add(tasks.poll());
            changed.signalAll();
        } finally {
            lock.unlock();
        }
        for (Thread t : workers) t.interrupt();  // stop tasks that are running right now
        return notStarted;
    }

    // Wait until every worker has stopped, but no longer than `timeout` in total.
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        for (Thread t : workers) {
            long left = deadline - System.currentTimeMillis();
            if (left <= 0) return false;
            t.join(left);
            if (t.isAlive()) return false;
        }
        return true;
    }

    // ---------------- worker side (same shape as the scheduler) ----------------

    private void workerLoop() {
        while (true) {
            Runnable task;
            try {
                task = take();
            } catch (InterruptedException e) {
                return;                          // shutdownNow() interrupted an idle worker
            }
            if (task == null) return;            // shut down and nothing left

            try {
                task.run();                      // run OUTSIDE the lock
                finish(null);
            } catch (RuntimeException e) {
                finish(e);                       // a failing task must not kill the worker
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
            Runnable task = tasks.poll();
            changed.signalAll();                 // a slot freed up: a waiting execute() can add
            return task;
        } finally {
            lock.unlock();
        }
    }

    // Called when a task ends, successfully (e == null) or not.
    private void finish(Exception e) {
        if (e != null) {
            System.out.println(Thread.currentThread().getName() + ": task failed: " + e);
        }
    }
}