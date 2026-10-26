package com.example.concurrency.interview.executors;
import java.util.*;
import java.util.concurrent.locks.*;
public class PriorityExecutor {

    private static class Job{
        private int priority;
        private Runnable task;

        Job(int priority, Runnable task){
            this.priority=priority;
            this.task=task;
        }
    }
    private PriorityQueue<Job> queue =new PriorityQueue<>((a,b) -> Integer.compare(b.priority,a.priority));
    private List<Thread> workers=new ArrayList<>();
    private Exception error;
    private boolean isShutdown=false;

    ReentrantLock lock =new ReentrantLock();
    Condition isChanged = lock.newCondition();


    public PriorityExecutor(int numThreads) {

        for(int i=0;i<numThreads;i++){
            Thread thread = new Thread(this::workerLoop);
            workers.add(thread);
            thread.start();
        }
    }

    private void workerLoop(){
        Job job;
        while(true){
            try {
                job = getJob();
            }
            catch (InterruptedException e){
                return ;
            }
            if(job==null)return;
            try{
                job.task.run();
               finish(job, null);
            }
            catch(Exception e){
                finish(job, e);
            }
        }
    }

    private Job getJob() throws InterruptedException{
        lock.lockInterruptibly();
        Job job;
        try{
            while(!isShutdown && queue.isEmpty())isChanged.await();
            if(queue.isEmpty())return null;
            job= queue.poll();

            return job;

        }
        finally{
            lock.unlock();
        }
    }

    void finish(Job job, Exception e){
        lock.lock();
        try{
            if(error==null)error=e;
        }
        finally{
            lock.unlock();
        }


    }

    // Higher number = more urgent. Returns immediately.
    public void submit(Runnable task, int priority)  {

        if(task==null)throw new IllegalArgumentException();

        lock.lock();
        try {

            if (isShutdown) return;
            queue.add(new Job(priority, task));
            isChanged.signalAll();
        }
        finally{
            lock.unlock();
        }

    }

    // Stop accepting tasks; let all submitted tasks finish; return when all workers have exited.
    public void shutdown() throws InterruptedException {
        lock.lockInterruptibly();
        try{
            isShutdown=true;
            isChanged.signalAll();
        }
        finally{
            lock.unlock();
        }
        for(Thread worker : workers){
            worker.join();
        }

    }
}
