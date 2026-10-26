package com.example.concurrency.interview.resourcesharing.fairplayground;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class FairPlayground {
    private final int maxPlayers;
    private int size = 0;                 // players inside right now
    private String currentTeam = null;    // team inside right now (null when empty)
    private String nextTeam = null;       // NEW: a waiting team that has claimed the next turn

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition canEnter = lock.newCondition();

    public FairPlayground(int maxPlayers) {
        if (maxPlayers <= 0) throw new IllegalArgumentException("maxPlayers must be > 0");
        this.maxPlayers = maxPlayers;
    }

    public void enter(String team) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            try {
                while (!allowedToEnter(team)) {
                    // Another team is playing and nobody has claimed the next turn yet: claim it for my team.
                    if (size > 0 && !team.equals(currentTeam) && nextTeam == null) {
                        nextTeam = team;
                    }
                    canEnter.await();
                }
            } catch (InterruptedException e) {
                if (team.equals(nextTeam)) {      // giving up: release my team's claim
                    nextTeam = null;              // (another waiting player of my team can claim it again)
                    canEnter.signalAll();
                }
                throw e;
            }
            size++;
            currentTeam = team;
            if (team.equals(nextTeam)) {
                nextTeam = null;                  // my team's turn has started: the claim is used up
            }
        } finally {
            lock.unlock();
        }
    }

    // Called with the lock held.
    private boolean allowedToEnter(String team) {
        if (size == maxPlayers) {
            return false;                                       // full
        }
        if (size == 0) {
            return nextTeam == null || nextTeam.equals(team);   // empty: whoever has the next turn (or anyone)
        }
        return team.equals(currentTeam) && nextTeam == null;    // join my team, unless another team is next
    }

    public void leave(String team) {
        lock.lock();
        try {
            if (size == 0 || !currentTeam.equals(team)) {
                throw new IllegalStateException("player of team " + team + " is not inside");
            }
            size--;
            if (size == 0) {
                currentTeam = null;
            }
            canEnter.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
