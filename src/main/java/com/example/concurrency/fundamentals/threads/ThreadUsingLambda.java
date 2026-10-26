package com.example.concurrency.fundamentals.threads;
public class ThreadUsingLambda {

    public static void main(String[] args) {
        Runnable runnable = () -> {
            String threadName = Thread.currentThread().getName();
            System.out.println(threadName);

        };
        Thread thread = new Thread(runnable, "thread1");
        thread.start();

    }
}
