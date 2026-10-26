package com.example.concurrency.interview.filecopy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

// READERS = Pattern A (take a block number -> read it -> finish).
// Readers -> bounded queue -> ONE WRITER = Pattern B.
public class FloppyToSsd {
    private static final int BLOCK_SIZE = FakeFS.BLOCK_SIZE;
    private static final int BATCH_BYTES = 64 * 1024;       // write in 64 KB pieces

    private static class Chunk {
        final long offset;
        final byte[] data;
        Chunk(long offset, byte[] data) { this.offset = offset; this.data = data; }
    }

    private final FakeFS fs;
    private final String srcPath, dstPath;
    private int src, dst;                  // file ids, set in copy() before threads start
    private final BlockingQueue<Chunk> queue = new ArrayBlockingQueue<>(64);  // bounded: limits memory

    private long nextBlock = 0;            // next block number to hand out
    private boolean reachedEnd = false;    // a reader saw the end of the file
    private int activeReaders = 0;         // readers still running
    private Exception error = null;        // first failure (reader OR writer)
    private final ReentrantLock lock = new ReentrantLock();   // protects the 4 fields above

    public FloppyToSsd(FakeFS fs, String srcPath, String dstPath) {
        this.fs = fs;
        this.srcPath = srcPath;
        this.dstPath = dstPath;
    }

    public void copy(int numReaders) throws Exception {
        src = fs.fopen(srcPath);
        dst = fs.fopen(dstPath);
        activeReaders = numReaders;
        try {
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < numReaders; i++) threads.add(new Thread(this::readerLoop));
            threads.add(new Thread(this::writerLoop));
            for (Thread t : threads) t.start();
            for (Thread t : threads) t.join();
        } finally {
            fs.fclose(src);
            fs.fclose(dst);
        }
        if (error != null) throw error;
    }

    // ================= READERS (same shape as the scheduler) =================

    private void readerLoop() {
        try {
            while (true) {
                long block = take();
                if (block == -1) return;                 // end of file reached, or someone failed

                try {
                    boolean wasLast = readBlock(block);  // the work, OUTSIDE the lock
                    finish(wasLast, null);
                } catch (Exception e) {
                    finish(false, e);
                }
            }
        } finally {
            readerFinished();                            // "I will put nothing more in the queue"
        }
    }

    // Give a reader the next block number, or -1 if there is no more work. Never waits.
    private long take() {
        lock.lock();
        try {
            if (reachedEnd || error != null) return -1;
            return nextBlock++;
        } finally {
            lock.unlock();
        }
    }

    // Called when a block ends: record the end of file or the first failure.
    private void finish(boolean wasLast, Exception e) {
        lock.lock();
        try {
            if (e != null && error == null) error = e;
            if (wasLast) reachedEnd = true;
        } finally {
            lock.unlock();
        }
    }

    // Read one block and hand it to the writer. Returns true if this was the last block.
    private boolean readBlock(long block) throws IOException, InterruptedException {
        long offset = block * BLOCK_SIZE;
        byte[] data = fs.read(src, offset);
        if (data.length == 0) return true;               // past the end: nothing to send
        putInQueue(new Chunk(offset, data));
        return data.length < BLOCK_SIZE;                 // partial block = the last one
    }

    // Like queue.put, but gives up if someone failed (a dead writer would never make room).
    private void putInQueue(Chunk chunk) throws InterruptedException {
        while (!queue.offer(chunk, 100, TimeUnit.MILLISECONDS)) {
            if (hasError()) return;
        }
    }

    // ================= WRITER (one thread) =================

    private void writerLoop() {
        Map<Long, byte[]> pending = new HashMap<>();                 // arrived early: offset -> bytes
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();  // in-order bytes not yet written
        long bufferStart = 0;                                         // file offset of buffer's first byte
        long nextOffset = 0;                                          // next offset we need, in order
        try {
            while (true) {
                Chunk c = takeChunk();
                if (c == null) break;                                 // all readers done and queue empty, or error

                pending.put(c.offset, c.data);
                while (pending.containsKey(nextOffset)) {             // move in-order blocks into the buffer
                    byte[] data = pending.remove(nextOffset);
                    buffer.write(data, 0, data.length);
                    nextOffset += data.length;
                }
                if (buffer.size() >= BATCH_BYTES) {                   // big enough -> one big write
                    fs.write(dst, bufferStart, buffer.toByteArray());
                    bufferStart = nextOffset;
                    buffer.reset();
                }
            }
            if (buffer.size() > 0 && !hasError()) {                  // last, smaller piece
                fs.write(dst, bufferStart, buffer.toByteArray());
            }
        } catch (Exception e) {
            finish(false, e);                                         // same finish: record the failure
        }
    }

    // The writer's take(): next chunk from the queue, or null when there is nothing more to come.
    private Chunk takeChunk() throws InterruptedException {
        while (true) {
            if (hasError()) return null;
            boolean readersDone = allReadersFinished();   // read BEFORE polling (see notes)
            Chunk c = queue.poll(100, TimeUnit.MILLISECONDS);
            if (c != null) return c;
            if (readersDone) return null;                 // no readers left and nothing in the queue
        }
    }

    // ================= shared state helpers (always under the lock) =================

    private void readerFinished() {
        lock.lock();
        try { activeReaders--; } finally { lock.unlock(); }
    }

    private boolean allReadersFinished() {
        lock.lock();
        try { return activeReaders == 0; } finally { lock.unlock(); }
    }

    private boolean hasError() {
        lock.lock();
        try { return error != null; } finally { lock.unlock(); }
    }
}