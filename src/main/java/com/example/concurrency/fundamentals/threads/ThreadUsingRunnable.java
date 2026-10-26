package com.example.concurrency.fundamentals.threads;
public class ThreadUsingRunnable {

    public static class MyRunnable implements Runnable {
        @Override
        public void run() {
            System.out.println("MyRunnable run");
        }
    }
    public static void main(String[] args) {
        Thread thread =new Thread(new MyRunnable());
        thread.start();
    }
}
