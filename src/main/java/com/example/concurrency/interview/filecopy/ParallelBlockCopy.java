package com.example.concurrency.interview.filecopy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelBlockCopy {
    private static final int BLOCK_SIZE = FakeFS.BLOCK_SIZE;

    private final FakeFS fs;
    private final String srcPath, dstPath;
    private int src, dst;                  // file ids, set in copy() before workers start
    private long nextBlock = 0;            // next block number to hand out
    private boolean reachedEnd = false;    // someone saw the end of the file
    private Exception error = null;        // first failure
    private final ReentrantLock lock = new ReentrantLock();   // protects nextBlock, reachedEnd, error

    public ParallelBlockCopy(FakeFS fs, String srcPath, String dstPath) {
        this.fs = fs;
        this.srcPath = srcPath;
        this.dstPath = dstPath;
    }

    public void copy(int numWorkers) throws Exception {
        src = fs.fopen(srcPath);
        dst = fs.fopen(dstPath);
        try {
            List<Thread> workers = new ArrayList<>();
            for (int i = 0; i < numWorkers; i++) {
                Thread worker = new Thread(this::workerLoop);
                workers.add(worker);
                worker.start();
            }
            for (Thread worker : workers) {
                worker.join();
            }
        } finally {
            fs.fclose(src);                // always close, even on error
            fs.fclose(dst);
        }
        if (error != null) throw error;
    }

    private void workerLoop() {
        try {
            while (true) {
                long block = takeBlock();
                if (block == -1) return;

                long offset = block * BLOCK_SIZE;
                byte[] data = fs.read(src, offset);   // one call returns the whole block

                if (data.length == 0) {               // past the end: nothing to copy
                    markEnd();
                    return;
                }
                fs.write(dst, offset, data);          // same offset in the destination
                if (data.length < BLOCK_SIZE) {       // partial block = the last one
                    markEnd();
                    return;
                }
            }
        } catch (Exception e) {
            setError(e);
        }
    }

    // Hand out the next block number, or -1 if there is no more work.
    private long takeBlock() {
        lock.lock();
        try {
            if (reachedEnd || error != null) return -1;
            return nextBlock++;
        } finally {
            lock.unlock();
        }
    }

    private void markEnd() {
        lock.lock();
        try {
            reachedEnd = true;
        } finally {
            lock.unlock();
        }
    }

    private void setError(Exception e) {
        lock.lock();
        try {
            if (error == null) error = e;
        } finally {
            lock.unlock();
        }
    }
}