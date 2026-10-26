package com.example.concurrency.interview.filecopy;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

// Pretend file system. In the interview, fopen/fclose/read/write are GIVEN; this class only exists so we can test.
public class FakeFS {
    public static final int BLOCK_SIZE = 4096;

    private final Map<String, byte[]> files = new HashMap<>();   // path -> file bytes
    private final Map<Integer, String> openFiles = new HashMap<>(); // file id -> path
    private int nextId = 0;

    public synchronized void createFile(String path, byte[] content) {   // for testing
        files.put(path, content.clone());
    }

    public synchronized byte[] content(String path) {                    // for testing
        return files.getOrDefault(path, new byte[0]).clone();
    }

    // Open (or create) a file and return its id.
    public synchronized int fopen(String path) {
        files.putIfAbsent(path, new byte[0]);
        int id = nextId++;
        openFiles.put(id, path);
        return id;
    }

    public synchronized void fclose(int id) {
        openFiles.remove(id);
    }

    // Read the block that starts at offset. Returns up to BLOCK_SIZE bytes; an empty array past the end.
    public synchronized byte[] read(int id, long offset) throws IOException {
        byte[] data = files.get(pathOf(id));
        if (offset >= data.length) return new byte[0];
        int end = (int) Math.min(data.length, offset + BLOCK_SIZE);
        return Arrays.copyOfRange(data, (int) offset, end);
    }

    // Write bytes at offset, growing the file if needed.
    public synchronized void write(int id, long offset, byte[] bytes) throws IOException {
        String path = pathOf(id);
        byte[] data = files.get(path);
        int end = (int) offset + bytes.length;
        if (end > data.length) data = Arrays.copyOf(data, end);
        System.arraycopy(bytes, 0, data, (int) offset, bytes.length);
        files.put(path, data);
    }

    private String pathOf(int id) throws IOException {
        String path = openFiles.get(id);
        if (path == null) throw new IOException("file " + id + " is not open");
        return path;
    }
}
