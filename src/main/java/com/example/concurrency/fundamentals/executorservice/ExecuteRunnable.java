package com.example.concurrency.fundamentals.executorservice;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecuteRunnable {

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                System.out.println(Thread.currentThread().getName() + ": runnable");
            }
        };

        // fire and forget: no result, no way to know when it finished
        executorService.execute(runnable);

        // without this the worker thread keeps waiting for tasks and the program never exits
        executorService.shutdown();
    }
}
