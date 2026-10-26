package com.example.concurrency.interview.locks.readwritelock.mockinterview;
import java.util.ArrayList;
import java.util.List;

public class ReadWriteLockDemo {
    public static void main(String[] args) throws InterruptedException {
        SharedCounter sharedCounter = new SharedCounter();      // local: visible inside the lambdas
        List<Thread> threads = new ArrayList<>();

        // 5 readers
        for (int i = 0; i < 5; i++) {
            Thread reader = new Thread(() -> {
                try {
                    for (int j = 0; j < 200; j++) {
                        sharedCounter.read();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();          // restore the flag, then exit
                }
            }, "reader-" + i);
            threads.add(reader);
        }

        // 2 writers
        for (int i = 0; i < 2; i++) {
            Thread writer = new Thread(() -> {
                try {
                    for (int j = 0; j < 200; j++) {
                        sharedCounter.increment();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "writer-" + i);
            threads.add(writer);
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();                      // wait for everyone

        System.out.println("final count = " + sharedCounter.read() + " (expected 400)");
    }
}