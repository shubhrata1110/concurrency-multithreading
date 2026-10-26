package com.example.concurrency.interview.filecopy.preadpwrite;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelCopy {
    private static final int CHUNK_SIZE = 4096;

    private final FakeFile srcFile, dstFile;
    private final long fileSize;
    private final long numChunks;
    private long nextChunk = 0;          // next chunk number to hand out
    private Exception error = null;      // first failure
    private final ReentrantLock lock = new ReentrantLock();   // protects nextChunk and error

    public ParallelCopy(FakeFile srcFile, FakeFile dstFile) {
        this.srcFile = srcFile;
        this.dstFile = dstFile;
        this.fileSize = srcFile.size();
        this.numChunks = (fileSize + CHUNK_SIZE - 1) / CHUNK_SIZE;   // round up
    }

    public void copy(int numWorkers) throws Exception {
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
            Thread worker = new Thread(this::workerLoop);
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        if (error != null) throw error;
    }

    // ---------------- worker side (same shape as the scheduler) ----------------

    private void workerLoop() {
        byte[] buf = new byte[CHUNK_SIZE];       // each thread has its OWN buffer
        while (true) {
            long chunk = take();
            if (chunk == -1) return;             // no more chunks, or someone failed

            try {
                copyChunk(chunk, buf);           // the work, OUTSIDE the lock
                finish(null);
            } catch (Exception e) {
                finish(e);
            }
        }
    }

    // Give a worker the next chunk number, or -1 if there is no more work. Never waits.
    private long take() {
        lock.lock();
        try {
            if (error != null || nextChunk == numChunks) return -1;
            return nextChunk++;
        } finally {
            lock.unlock();
        }
    }

    // Called when a chunk ends, successfully (e == null) or not.
    private void finish(Exception e) {
        if (e == null) return;                   // success: nothing shared to update
        lock.lock();
        try {
            if (error == null) error = e;        // keep the first failure
        } finally {
            lock.unlock();
        }
    }

    private void copyChunk(long chunk, byte[] buf) throws IOException {
        long offset = chunk * CHUNK_SIZE;
        int len = (int) Math.min(CHUNK_SIZE, fileSize - offset);   // last chunk may be smaller
        readFully(buf, len, offset);
        dstFile.pwrite(buf, len, offset);
    }

    // pread may return fewer bytes than asked, so keep reading until we have len bytes.
    private void readFully(byte[] buf, int len, long offset) throws IOException {
        int total = 0;
        while (total < len) {
            int n = srcFile.pread(buf, total, len - total, offset + total);
            if (n == 0) throw new IOException("file ended early at " + (offset + total));
            total += n;
        }
    }
}