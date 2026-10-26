package com.example.concurrency.fundamentals.threadpool;
import java.util.concurrent.BlockingQueue;

public class PoolThreadRunnable implements Runnable {

    private Thread                  thread    = null;
    private BlockingQueue<Runnable> taskQueue = null;
    private boolean                 isStopped = false;

    public PoolThreadRunnable(BlockingQueue<Runnable> queue){
        taskQueue = queue;
    }

    public void run(){
        synchronized (this) {
            this.thread = Thread.currentThread();   // under the lock, so doStop() sees it
        }
        while(!isStopped()){
            try{
                Runnable runnable = taskQueue.take();
                runnable.run();
            } catch(Exception e){
                //log or otherwise report exception,
                //but keep pool thread alive.
            }
        }
    }

    public synchronized void doStop(){
        isStopped = true;
        //break pool thread out of dequeue() call.
        //thread is null if run() hasn't started yet; it will see isStopped and exit right away.
        if (this.thread != null) {
            this.thread.interrupt();
        }
    }

    public synchronized boolean isStopped(){
        return isStopped;
    }
}
