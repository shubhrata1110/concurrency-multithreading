package com.example.concurrency.interview.resourcesharing.fairplayground;
public class FairPlaygroundSync {
    private final int maxPlayers;
    private int size = 0;
    private String currentTeam = null;
    private String nextTeam = null;       // a waiting team that has claimed the next turn

    public FairPlaygroundSync(int maxPlayers) {
        if (maxPlayers <= 0) throw new IllegalArgumentException("maxPlayers must be > 0");
        this.maxPlayers = maxPlayers;
    }

    public synchronized void enter(String team) throws InterruptedException {
        try {
            while (!allowedToEnter(team)) {
                // another team is playing and nobody has claimed the next turn: claim it
                if (size > 0 && !team.equals(currentTeam) && nextTeam == null) {
                    nextTeam = team;
                }
                wait();
            }
        } catch (InterruptedException e) {
            if (team.equals(nextTeam)) {      // giving up: release my team's claim
                nextTeam = null;
                notifyAll();
            }
            throw e;
        }
        size++;
        currentTeam = team;
        if (team.equals(nextTeam)) {
            nextTeam = null;                  // my team's turn started: claim used up
        }
    }

    // called while holding the lock (only from synchronized enter)
    private boolean allowedToEnter(String team) {
        if (size == maxPlayers) return false;
        if (size == 0) return nextTeam == null || nextTeam.equals(team);
        return team.equals(currentTeam) && nextTeam == null;
    }

    public synchronized void leave(String team) {
        if (size == 0 || !currentTeam.equals(team)) {
            throw new IllegalStateException("player of team " + team + " is not inside");
        }
        size--;
        if (size == 0) currentTeam = null;
        notifyAll();
    }
}