package com.example.concurrency.fundamentals.executorservice;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ShutdownExample {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("--- shutdown() ---");
        ExecutorService executorService = createAndFill();
        // stop accepting new tasks, but finish all tasks already submitted
        executorService.shutdown();
        // main waits here until every task is done (or 10 seconds pass)
        boolean finished = executorService.awaitTermination(10, TimeUnit.SECONDS);
        System.out.println("all tasks finished = " + finished);

        System.out.println("--- shutdownNow() ---");
        ExecutorService executorService2 = createAndFill();
        Thread.sleep(50);
        // interrupt running tasks and throw away tasks still in the queue
        List<Runnable> neverRan = executorService2.shutdownNow();
        System.out.println("tasks that never ran = " + neverRan.size());
        executorService2.awaitTermination(10, TimeUnit.SECONDS);
    }

    private static ExecutorService createAndFill() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 6; i++) {
            int taskNo = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(200);
                        System.out.println(Thread.currentThread().getName() + ": Task " + taskNo + " done");
                    } catch (InterruptedException e) {
                        System.out.println(Thread.currentThread().getName() + ": Task " + taskNo + " interrupted");
                    }
                }
            });
        }
        return executorService;
    }
}
