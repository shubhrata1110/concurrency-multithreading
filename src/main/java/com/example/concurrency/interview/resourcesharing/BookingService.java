package com.example.concurrency.interview.resourcesharing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class Seat {
    final String id;
    private String bookedBy = null;     // null = free
    private int version = 0;            // +1 on every change

    Seat(String id) { this.id = id; }

    // READ: a consistent snapshot of the seat (version + free?)
    synchronized int version() { return version; }
    synchronized boolean isFree() { return bookedBy == null; }

    // VALIDATE + WRITE in one step: succeeds only if nobody changed the seat since we read 'expectedVersion'.
    synchronized boolean bookIfUnchanged(int expectedVersion, String user) {
        if (version != expectedVersion || bookedBy != null) return false;   // someone got there first
        bookedBy = user;
        version++;
        return true;
    }
}

class BookingService {
    private final Map<String, Seat> seats = new HashMap<>();     // filled once, never changed after

    BookingService(List<String> seatIds) {
        for (String id : seatIds) seats.put(id, new Seat(id));
    }

    // Returns true if 'user' got the seat.
    public boolean book(String seatId, String user) {
        Seat seat = seats.get(seatId);
        if (seat == null) throw new IllegalArgumentException("no seat " + seatId);

        // 1. READ (no lock held afterwards)
        int seenVersion = seat.version();
        if (!seat.isFree()) return false;

        // 2. SLOW WORK without holding any lock: price, offers, payment authorisation...
        doSlowWork();

        // 3. VALIDATE + WRITE: only succeeds if the seat is unchanged since step 1
        return seat.bookIfUnchanged(seenVersion, user);
    }

    // stands in for pricing, offers, payment authorisation...
    private void doSlowWork() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
