package com.example.concurrency.interview.scheduling.jobscheduler;
import java.util.*;

public class JobSchedulerSync {
    private final Queue<Task> readyTasks = new ArrayDeque<>();
    private final Set<String> finishedTaskIds = new HashSet<>();
    private final Set<String> seenTaskIds = new HashSet<>();
    private int runningCount = 0;
    private Exception error = null;
    private final TaskSource source;

    public JobSchedulerSync(TaskSource source) {
        this.source = source;
    }

    public void runAll(int maxWorkers) throws Exception {
        synchronized (this) {                    // block, not method: we join below
            addToReadyTasks();
        }

        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < maxWorkers; i++) {
            Thread worker = new Thread(this::workerLoop);
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();                       // wait for all workers to exit
        }

        synchronized (this) {
            if (error != null) throw error;
        }
    }

    private void workerLoop() {
        while (true) {
            Task task;
            try {
                task = take();
            } catch (InterruptedException e) {
                return;
            }
            if (task == null) return;            // all done, or an error happened
            try {
                task.doWork();                   // outside the lock
                finish(task, null);
            } catch (Exception e) {
                finish(task, e);
            }
        }
    }

    private synchronized Task take() throws InterruptedException {
        // wait while: no error, nothing ready, but someone is running (may unlock more tasks)
        while (error == null && readyTasks.isEmpty() && runningCount > 0) {
            wait();
        }
        if (error != null || readyTasks.isEmpty()) return null;
        runningCount++;
        return readyTasks.poll();
    }

    private synchronized void finish(Task task, Exception e) {
        runningCount--;
        if (e != null) {
            if (error == null) error = e;        // keep the first error
        } else {
            finishedTaskIds.add(task.taskID());
            addToReadyTasks();
        }
        notifyAll();                             // wake waiting workers: new tasks, error, or all done
    }

    // called with the lock held
    private void addToReadyTasks() {
        for (Task t : source.getNextTasks(finishedTaskIds)) {
            if (seenTaskIds.add(t.taskID())) {   // false if already seen
                readyTasks.add(t);
            }
        }
    }
}
