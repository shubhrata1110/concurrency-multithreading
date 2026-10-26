package com.example.concurrency.interview.datastructures;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SnapshotKeyValueStore<K, V> {
    // For every key: version -> value written in that version (null value = deleted)
    private final Map<K, TreeMap<Integer, V>> history = new HashMap<>();
    private int currentVersion = 0;                            // new writes go into this version
    private final TreeSet<Integer> liveSnapshots = new TreeSet<>();
    private final ReentrantReadWriteLock rw = new ReentrantReadWriteLock();

    public void put(K key, V value) {
        rw.writeLock().lock();
        try {
            history.computeIfAbsent(key, k -> new TreeMap<>()).put(currentVersion, value);
        } finally {
            rw.writeLock().unlock();
        }
    }

    public void delete(K key) {
        rw.writeLock().lock();
        try {
            TreeMap<Integer, V> versions = history.get(key);
            if (versions != null) versions.put(currentVersion, null);   // tombstone: old snapshots still see the old value
        } finally {
            rw.writeLock().unlock();
        }
    }

    // Latest value.
    public V get(K key) {
        rw.readLock().lock();
        try {
            TreeMap<Integer, V> versions = history.get(key);
            return versions == null ? null : versions.lastEntry().getValue();
        } finally {
            rw.readLock().unlock();
        }
    }

    // Freeze everything written so far. Later writes go into the next version.
    public int takeSnapshot() {
        rw.writeLock().lock();
        try {
            liveSnapshots.add(currentVersion);
            return currentVersion++;
        } finally {
            rw.writeLock().unlock();
        }
    }

    // Value as it was when snapshot snapId was taken.
    public V get(K key, int snapId) {
        rw.readLock().lock();
        try {
            if (!liveSnapshots.contains(snapId)) throw new IllegalArgumentException("no snapshot " + snapId);
            TreeMap<Integer, V> versions = history.get(key);
            if (versions == null) return null;
            Map.Entry<Integer, V> e = versions.floorEntry(snapId);   // last write with version <= snapId
            return e == null ? null : e.getValue();
        } finally {
            rw.readLock().unlock();
        }
    }

    public void deleteSnapshot(int snapId) {
        rw.writeLock().lock();
        try {
            if (!liveSnapshots.remove(snapId)) throw new IllegalArgumentException("no snapshot " + snapId);
//            // called with the write lock held
//private void cleanUp() {
//    Iterator<Map.Entry<String, TreeMap<Integer, String>>> it = data.entrySet().iterator();
//    while (it.hasNext()) {
//        TreeMap<Integer, String> versions = it.next().getValue();
//
//        List<Integer> toRemove = new ArrayList<>();
//        for (int v : versions.keySet()) {
//            Integer next = versions.higherKey(v);
//            if (next == null) break;                    // latest version: always keep
//            Integer s = liveSnapshots.ceiling(v);       // smallest snapshot ≥ v
//            if (s == null || s >= next) {               // no snapshot in [v, next)
//                toRemove.add(v);
//            }
//        }
//        for (int v : toRemove) versions.remove(v);      // remove AFTER the loop
//
//        // only a tombstone left → the key reads as "missing" everywhere → drop it
//        if (versions.size() == 1 && versions.firstEntry().getValue() == null) {
//            it.remove();
//        }
//    }
//}
        } finally {
            rw.writeLock().unlock();
        }
    }
}