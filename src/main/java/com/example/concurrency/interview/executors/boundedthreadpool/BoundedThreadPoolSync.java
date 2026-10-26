package com.example.concurrency.interview.executors.boundedthreadpool;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class BoundedThreadPoolSync {
    private final Queue<Runnable> tasks = new ArrayDeque<>();
    private final List<Thread> workers = new ArrayList<>();
    private boolean shutdown = false;
    private final int capacity;

    public BoundedThreadPoolSync(int numWorkers, int capacity) {
        this.capacity = capacity;
        for (int i = 0; i < numWorkers; i++) {
            Thread worker = new Thread(this::workerLoop, "worker-" + i);
            workers.add(worker);
            worker.start();
        }
    }

    // Add a task. Waits while the queue is full. Refused after shutdown.
    public synchronized void execute(Runnable task) throws InterruptedException {
        if (task == null) throw new NullPointerException("task is null");
        while (tasks.size() == capacity && !shutdown) {
            wait();                                   // releases the lock while waiting
        }
        if (shutdown) throw new IllegalStateException("pool is shut down");
        tasks.add(task);
        notifyAll();                                  // ONE wait set: wake everyone, they re-check
    }

    private void workerLoop() {
        while (true) {
            Runnable task;
            try {
                task = take();
            } catch (InterruptedException e) {
                return;                               // shutdownNow() interrupted an idle worker
            }
            if (task == null) return;                 // shut down and nothing left

            try {
                task.run();                           // OUTSIDE the lock (take() already returned)
                finish(null);
            } catch (RuntimeException e) {
                finish(e);
            }
        }
    }

    private synchronized Runnable take() throws InterruptedException {
        while (tasks.isEmpty() && !shutdown) {
            wait();
        }
        if (tasks.isEmpty()) return null;
        Runnable task = tasks.poll();
        notifyAll();                                  // a slot freed: a waiting execute() may continue
        return task;
    }

    private void finish(Exception e) {
        if (e != null) {
            System.out.println(Thread.currentThread().getName() + ": task failed: " + e);
        }
    }

    public synchronized void shutdown() {
        shutdown = true;
        notifyAll();                                  // idle workers exit, blocked execute() calls throw
    }

    public List<Runnable> shutdownNow() {
        List<Runnable> notStarted = new ArrayList<>();
        synchronized (this) {
            shutdown = true;
            while (!tasks.isEmpty()) notStarted.add(tasks.poll());
            notifyAll();
        }
        for (Thread worker : workers) worker.interrupt();   // outside the lock
        return notStarted;
    }

    // NOT synchronized: joining while holding the lock would block workers from take()/finish -> deadlock
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        for (Thread worker : workers) {
            long left = deadline - System.currentTimeMillis();
            if (left <= 0) return false;
            worker.join(left);
            if (worker.isAlive()) return false;
        }
        return true;
    }
}