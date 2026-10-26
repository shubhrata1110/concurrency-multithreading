package com.example.concurrency.fundamentals.executorservice;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CancelTask {
    public static void main(String[] args) throws InterruptedException {
        // only 1 thread, so the second task has to wait in the queue
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        Runnable slowTask = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println("slow task interrupted");
                }
                System.out.println("slow task finished");
            }
        };
        Runnable waitingTask = new Runnable() {
            @Override
            public void run() {
                System.out.println("waiting task ran");
            }
        };

        executorService.submit(slowTask);
        Future<?> future = executorService.submit(waitingTask);

        // waitingTask has not started yet, so it can be cancelled; false = don't interrupt if already running
        boolean cancelled = future.cancel(false);
        System.out.println("cancelled = " + cancelled);
        System.out.println("isCancelled = " + future.isCancelled());

        executorService.shutdown();
    }
}
