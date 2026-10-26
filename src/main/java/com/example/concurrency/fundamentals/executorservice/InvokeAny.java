package com.example.concurrency.fundamentals.executorservice;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InvokeAny {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);

        List<Callable<String>> callables = new ArrayList<>();
        callables.add(new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.sleep(300);
                return "Task 1";
            }
        });
        callables.add(new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.sleep(100);
                return "Task 2";
            }
        });
        callables.add(new Callable<String>() {
            @Override
            public String call() throws Exception {
                Thread.sleep(200);
                return "Task 3";
            }
        });

        // returns the result of the first task to finish; the others are cancelled
        String result = executorService.invokeAny(callables);
        System.out.println("result = " + result);

        executorService.shutdown();
    }
}
