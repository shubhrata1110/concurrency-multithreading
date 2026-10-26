package com.example.concurrency.interview.locks.readwritelock;
import java.util.*;

public class ReadWriteLockDemo {
    public static void main(String[] args) throws InterruptedException {
        SharedCounter sharedCounter =new SharedCounter();
        List<Thread> threads = new ArrayList<Thread>();

        for (int i = 0; i < 5; i++) {
            Thread readthread= new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = 0; i < 200; i++) {
                            int val = sharedCounter.read();
                            System.out.println(Thread.currentThread().getName() + " read " + val);
                        }
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            },"readthread"+i);
            threads.add(readthread);
        }
        for (int i = 0; i < 2; i++) {
            Thread writeThread= new Thread(new Runnable() {
                @Override
                public void run() {
                    try{
                        for (int i = 0; i < 200; i++) {
                            sharedCounter.write();
                            System.out.println(Thread.currentThread().getName()+ i);
                        }

                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                }
            },"writeThread"+i);
            threads.add(writeThread);
        }

        for(Thread thread:threads){
            thread.start();
        }
        for(Thread thread:threads){
            thread.join();
        }

        System.out.println("final count = " + sharedCounter.read() + " (expect 400: 2 writers x 200)");
    }
}
