package com.example.concurrency.fundamentals.executorservice;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SubmitRunnable {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName() + ": running task");
            }
        };

        Future<?> future = executorService.submit(runnable);

        // main waits here until the task is done; a Runnable has no result, so get() returns null
        Object result = future.get();
        System.out.println("task done, future.get() = " + result);

        executorService.shutdown();
    }
}
