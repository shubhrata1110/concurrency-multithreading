package com.example.concurrency.interview.filecopy.preadpwrite;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

// Pretend file. In the interview, pread/pwrite/size are GIVEN; this class only exists so we can test.
public class FakeFile {
    private byte[] data;                 // the file's bytes
    private int length;                  // how many bytes the file has
    private final Random random = new Random();
    public long failAtOffset = -1;       // for testing: pread throws at this offset

    public FakeFile(byte[] content) {    // a file that already has content (source)
        data = content.clone();
        length = content.length;
    }

    public FakeFile() {                  // an empty file (destination)
        data = new byte[16];
        length = 0;
    }

    public synchronized long size() {
        return length;
    }

    // Read up to n bytes from the file at fileOffset into buf, starting at buf[bufStart].
    // Returns how many bytes were read (can be LESS than n). Returns 0 at end of file.
    public synchronized int pread(byte[] buf, int bufStart, int n, long fileOffset) throws IOException {
        if (fileOffset == failAtOffset) throw new IOException("disk error at offset " + fileOffset);
        if (fileOffset >= length) return 0;
        int canRead = (int) Math.min(n, length - fileOffset);
        int count = 1 + random.nextInt(canRead);           // simulate short reads
        System.arraycopy(data, (int) fileOffset, buf, bufStart, count);
        return count;
    }

    // Write the first n bytes of buf into the file at fileOffset.
    public synchronized void pwrite(byte[] buf, int n, long fileOffset) {
        int end = (int) fileOffset + n;
        if (end > data.length) {
            data = Arrays.copyOf(data, Math.max(end, data.length * 2));   // grow
        }
        System.arraycopy(buf, 0, data, (int) fileOffset, n);
        length = Math.max(length, end);
    }

    public synchronized byte[] content() {               // for testing
        return Arrays.copyOf(data, length);
    }
}