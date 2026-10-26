package com.example.concurrency.interview.datastructures.blockingqueue;
import java.util.*;

public class BoundedBlockingQueueDemo {

    static class Producer implements Runnable {
        private final BoundedBlockingQueue<Integer> queue;
        Producer(BoundedBlockingQueue<Integer> queue) { this.queue = queue; }

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

    static class Consumer implements Runnable {
        private final BoundedBlockingQueue<Integer> queue;
        long sum = 0;                         // only this thread writes it
        Consumer(BoundedBlockingQueue<Integer> queue) { this.queue = queue; }

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
        BoundedBlockingQueue<Integer> queue = new BoundedBlockingQueue<>(5);
        List<Thread> threads = new ArrayList<>();
        List<Consumer> consumers = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            threads.add(new Thread(new Producer(queue), "producer-" + i));
            Consumer c = new Consumer(queue);
            consumers.add(c);
            threads.add(new Thread(c, "consumer-" + i));
        }
        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();    // wait for everyone to finish

        long total = 0;
        for (Consumer c : consumers) total += c.sum;
        long expected = 3L * 1000 * 1001 / 2; // 3 producers × (1 + 2 + … + 1000)

        System.out.println(total == expected ? "PASS" : "FAIL: got " + total + ", expected " + expected);
        System.out.println("size at end = " + queue.size());   // should be 0
    }
}
