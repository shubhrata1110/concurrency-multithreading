package com.example.concurrency.interview.datastructures.blockingqueue;
import java.util.ArrayList;
import java.util.List;

public class BlockingQueueWithTimeoutDemo {
    public static class Producer implements Runnable {
        private BlockingQueueWithTimeout<Integer> queue;
       Producer(BlockingQueueWithTimeout<Integer> queue) {
           this.queue = queue;
       }
       @Override
        public void run() {
           try {
               for (int i = 1; i <= 1000; i++) {
                   queue.put(i);
               }
           } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
           }

       }
    }
    public static class Consumer implements Runnable {
        private BlockingQueueWithTimeout<Integer> queue;
        long sum = 0;
        Consumer(BlockingQueueWithTimeout<Integer> queue) {
            this.queue = queue;
        }
        @Override
        public void run() {
            try {
                for (int i = 0; i < 1000; i++) {
                    sum += queue.take();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        BlockingQueueWithTimeout<Integer> queue = new BlockingQueueWithTimeout<>(5);
        List<Thread> threads = new ArrayList<>();
        List<Consumer> consumers = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            Thread producer = new Thread(new Producer(queue), "Producer " + i);
            threads.add(producer);
            Consumer c = new Consumer(queue);
            consumers.add(c);
            Thread consumer = new Thread(c, "Consumer " + i);   // the same Consumer we sum up later
            threads.add(consumer);
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        long total = 0;
        for (Consumer c : consumers) total += c.sum;
        long expected = 3L * 1000 * 1001 / 2; // 3 producers × (1 + 2 + … + 1000)

        System.out.println(total == expected ? "PASS" : "FAIL: got " + total + ", expected " + expected);
        System.out.println("size at end = " + queue.size());   // should be 0
    }
}
