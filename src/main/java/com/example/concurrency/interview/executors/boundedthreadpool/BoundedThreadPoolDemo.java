package com.example.concurrency.interview.executors.boundedthreadpool;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BoundedThreadPoolDemo {
    public static int count = 0;

    public static class IncreaseCounter implements Runnable {
        @Override
        public void run() {
            synchronized (BoundedThreadPoolDemo.class) {
                count++;
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // Test 1: shutdown() finishes all queued tasks; a failing task doesn't kill a worker
        BoundedThreadPool pool = new BoundedThreadPool(5, 10);
        pool.execute(() -> { throw new RuntimeException("boom (expected)"); });
        for (int i = 0; i < 50; i++) {
            pool.execute(new IncreaseCounter());
        }
        pool.shutdown();
        boolean finished = pool.awaitTermination(3, TimeUnit.SECONDS);
        synchronized (BoundedThreadPoolDemo.class) {
            System.out.println("Test 1: terminated=" + finished + ", count=" + count + "  (expect true, 50)");
        }

        // Test 2: execute() after shutdown() is refused
        try {
            pool.execute(new IncreaseCounter());
            System.out.println("Test 2: FAIL, task was accepted after shutdown");
        } catch (IllegalStateException e) {
            System.out.println("Test 2: refused after shutdown  (expected)");
        }

        // Test 3: shutdownNow() returns the tasks that never started and stops a busy worker
        BoundedThreadPool pool2 = new BoundedThreadPool(1, 20);
        pool2.execute(() -> {
            try {
                Thread.sleep(5000);                      // a long task
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();      // restore the flag and end the task early
            }
        });
        for (int i = 0; i < 10; i++) {
            pool2.execute(new IncreaseCounter());
        }
        Thread.sleep(100);                               // give the worker time to start the long task
        List<Runnable> notStarted = pool2.shutdownNow();
        boolean finished2 = pool2.awaitTermination(1, TimeUnit.SECONDS);
        System.out.println("Test 3: notStarted=" + notStarted.size() + ", terminated=" + finished2
                + "  (expect 10, true)");
    }
}