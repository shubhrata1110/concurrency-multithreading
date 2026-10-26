package com.example.concurrency.fundamentals.threadpool;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPool {

    private BlockingQueue<Runnable> taskQueue = null;
    private List<PoolThreadRunnable> runnables = new ArrayList<>();
    private boolean isStopped = false;
    private final AtomicInteger pendingTasks = new AtomicInteger(0);   // submitted but not finished yet

    public ThreadPool(int noOfThreads, int maxNoOfTasks){
        taskQueue = new ArrayBlockingQueue<>(maxNoOfTasks);

        for(int i=0; i<noOfThreads; i++){
            PoolThreadRunnable poolThreadRunnable =
                    new PoolThreadRunnable(taskQueue);

            runnables.add(poolThreadRunnable);
        }
        for(PoolThreadRunnable runnable : runnables){
            new Thread(runnable).start();
        }
    }

    public synchronized void  execute(Runnable task) throws Exception{
        if(this.isStopped) throw
                new IllegalStateException("ThreadPool is stopped");

        pendingTasks.incrementAndGet();
        // wrap the task so we know when it has FINISHED, not just when it left the queue
        Runnable trackedTask = () -> {
            try {
                task.run();
            } finally {
                pendingTasks.decrementAndGet();
            }
        };
        // put() waits while the queue is full; offer() would silently drop the task
        this.taskQueue.put(trackedTask);
    }

    public synchronized void stop(){
        this.isStopped = true;
        for(PoolThreadRunnable runnable : runnables){
            runnable.doStop();
        }
    }

    // not synchronized: sleeping while holding the pool's lock would block execute() and stop()
    public void waitUntilAllTasksFinished() {
        while(this.pendingTasks.get() > 0) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

}
