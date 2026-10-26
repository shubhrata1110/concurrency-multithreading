package com.example.concurrency.fundamentals.executorservice;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SubmitCallable {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        // Callable is like Runnable, but call() returns a value
        Callable<String> callable = new Callable<String>() {
            @Override
            public String call() throws Exception {
                System.out.println(Thread.currentThread().getName() + ": running callable");
                return "Callable Result";
            }
        };

        Future<String> future = executorService.submit(callable);

        // main waits here until call() returns, then gets its return value
        String result = future.get();
        System.out.println("future.get() = " + result);

        executorService.shutdown();
    }
}
