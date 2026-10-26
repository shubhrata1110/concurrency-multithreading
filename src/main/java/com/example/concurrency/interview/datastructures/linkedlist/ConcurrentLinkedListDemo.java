package com.example.concurrency.interview.datastructures.linkedlist;
import java.util.ArrayList;
import java.util.List;

public class ConcurrentLinkedListDemo {

    public static void main(String[] args) throws InterruptedException {
        ConcurrentLinkedList list = new ConcurrentLinkedList();   // one shared list for all threads

        // ---------- Phase 1: 3 inserters + 4 searchers at the same time ----------
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            int start = i * 1000;                        // inserter 0 adds 0-999, 1 adds 1000-1999, 2 adds 2000-2999
            threads.add(new Thread(() -> {
                try {
                    for (int v = start; v < start + 1000; v++) {
                        list.insert(v);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "inserter-" + i));
        }

        for (int i = 0; i < 4; i++) {
            threads.add(new Thread(() -> {
                try {
                    for (int v = 0; v < 3000; v++) {
                        list.search(v);                  // may be true or false: inserts are still happening
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "searcher-" + i));
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        int found = countPresent(list);
        System.out.println("After inserting: found " + found + " of 3000 (expect 3000)");

        // ---------- Phase 2: 3 deleters + 2 searchers at the same time ----------
        threads.clear();

        for (int i = 0; i < 3; i++) {
            int start = i * 1000;                        // each deleter removes the range one inserter added
            threads.add(new Thread(() -> {
                try {
                    for (int v = start; v < start + 1000; v++) {
                        if (!list.delete(v)) {
                            System.out.println("ERROR: " + v + " was missing");
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "deleter-" + i));
        }

        for (int i = 0; i < 2; i++) {
            threads.add(new Thread(() -> {
                try {
                    for (int v = 0; v < 3000; v++) {
                        list.search(v);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "searcher-" + i));
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        found = countPresent(list);
        System.out.println("After deleting:  found " + found + " of 3000 (expect 0)");
    }

    // Searches every value 0-2999 from the main thread, after all workers are done
    private static int countPresent(ConcurrentLinkedList list) throws InterruptedException {
        int found = 0;
        for (int v = 0; v < 3000; v++) {
            if (list.search(v)) found++;
        }
        return found;
    }
}