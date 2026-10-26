package com.example.concurrency.interview.datastructures;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReentrantLock;

public class TwoQueuesSharedBuffer {
    private final int[] data;          // numBlocks * blockSize ints: the shared storage
    private final int blockSize;
    private final int[] nextBlock;     // per block: next block in the SAME list, or -1
    private int freeHead;              // first free block, or -1

    // per queue (index 0 or 1)
    private final int[] headBlock = {-1, -1};   // block we read from
    private final int[] headPos   = {0, 0};     // next position to read inside headBlock
    private final int[] tailBlock = {-1, -1};   // block we write into
    private final int[] tailPos   = {0, 0};     // next position to write inside tailBlock
    private final int[] size      = {0, 0};

    private final ReentrantLock lock = new ReentrantLock();

    public TwoQueuesSharedBuffer(int numBlocks, int blockSize) {
        this.blockSize = blockSize;
        data = new int[numBlocks * blockSize];   // all allocation happens here, once
        nextBlock = new int[numBlocks];
        for (int b = 0; b < numBlocks; b++) {    // every block starts in the free list: 0 -> 1 -> 2 -> ...
            nextBlock[b] = (b + 1 < numBlocks) ? b + 1 : -1;
        }
        freeHead = 0;
    }

    // Add value to queue q (0 or 1). Returns false if the whole buffer is full.
    public boolean put(int q, int value) {
        lock.lock();
        try {
            if (tailBlock[q] == -1 || tailPos[q] == blockSize) {   // no block yet, or tail block full
                int b = allocBlock();
                if (b == -1) return false;                          // no free block left anywhere
                if (tailBlock[q] == -1) {                           // queue was empty: b is head AND tail
                    headBlock[q] = b;
                    headPos[q] = 0;
                } else {
                    nextBlock[tailBlock[q]] = b;                    // link b after the old tail block
                }
                tailBlock[q] = b;
                tailPos[q] = 0;
            }
            data[tailBlock[q] * blockSize + tailPos[q]] = value;
            tailPos[q]++;
            size[q]++;
            return true;
        } finally {
            lock.unlock();
        }
    }

    // Remove the oldest value of queue q. Throws if that queue is empty.
    public int get(int q) {
        lock.lock();
        try {
            if (size[q] == 0) throw new NoSuchElementException("queue " + q + " is empty");
            int value = data[headBlock[q] * blockSize + headPos[q]];
            headPos[q]++;
            size[q]--;

            if (size[q] == 0) {                       // queue now empty: give its only block back
                freeBlock(headBlock[q]);
                headBlock[q] = tailBlock[q] = -1;
                headPos[q] = tailPos[q] = 0;
            } else if (headPos[q] == blockSize) {     // finished reading this block: move on, free it
                int used = headBlock[q];
                headBlock[q] = nextBlock[used];
                headPos[q] = 0;
                freeBlock(used);
            }
            return value;
        } finally {
            lock.unlock();
        }
    }

    // Caller holds the lock. Take a block from the free list, or -1 if none.
    private int allocBlock() {
        if (freeHead == -1) return -1;
        int b = freeHead;
        freeHead = nextBlock[b];
        nextBlock[b] = -1;
        return b;
    }

    // Caller holds the lock. Put block b back at the front of the free list.
    private void freeBlock(int b) {
        nextBlock[b] = freeHead;
        freeHead = b;
    }
}
