package com.example.concurrency.fundamentals.executorservice;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class InvokeAll {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(3);

        List<Callable<String>> callables = new ArrayList<>();
        for (int i = 1; i <= 3; i++) {
            int taskNo = i;
            callables.add(new Callable<String>() {
                @Override
                public String call() throws Exception {
                    return Thread.currentThread().getName() + ": Task " + taskNo;
                }
            });
        }

        // runs all tasks and waits until every one has finished; one Future per task, same order as the list
        List<Future<String>> futures = executorService.invokeAll(callables);

        for (Future<String> future : futures) {
            System.out.println("future.get() = " + future.get());
        }

        executorService.shutdown();
    }
}
