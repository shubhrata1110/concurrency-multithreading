package com.example.concurrency.interview.executors.boundedthreadpool;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BoundedThreadPoolPractice {
    private final Queue<Runnable>tasks =new ArrayDeque<>();
    private final List<Thread>workers=new ArrayList<>();
    private boolean shutdown=false;
    private final int capacity;
    private Exception error;

    ReentrantLock lock=new ReentrantLock();
    Condition isChanged=lock.newCondition();



    public BoundedThreadPoolPractice(int numWorkers, int capacity) {
        this.capacity = capacity;
        for (int i = 0; i < numWorkers; i++) {
            Thread worker = new Thread(this::workerLoop);
            workers.add(worker);
            worker.start();
        }
    }
    public void executeTask(Runnable task) throws InterruptedException {
        if(task==null){throw new NullPointerException();}
        lock.lock();
        try {
            while(tasks.size()>=capacity && !shutdown){
                isChanged.await();
            }
            if(shutdown)throw new IllegalStateException("pool is shut down");
            tasks.add(task);
            isChanged.signalAll();

        }
        finally {
            lock.unlock();
        }
    }
    private void workerLoop() {
        Runnable task;
        while(true){
            try{
                task= take();
            }
            catch(InterruptedException e) {
                return;

            }
            if(task==null)return;
            try{
                task.run();
                finish(null);
            }
            catch(RuntimeException e){
                finish(e);
            }
        }

    }

    private Runnable take() throws InterruptedException {
        lock.lockInterruptibly();
        try{
            while(!shutdown && tasks.isEmpty()){
                isChanged.await();
            }
            if(tasks.isEmpty()){
                return null;
            }
            Runnable task= tasks.poll();
            isChanged.signalAll();
            return task;
        }
        finally{
            lock.unlock();
        }

    }

    private void finish(Exception e){
        lock.lock();                       // error is shared by all workers
        try{
            if(error==null){
                error=e;
            }
        }
        finally{
            lock.unlock();
        }
    }

    public void shutdown(){
        lock.lock();
        try{
            shutdown=true;
            isChanged.signalAll();
        }
        finally{
            lock.unlock();
        }
    }

    public List<Runnable> shutdownNow(){
        ArrayList<Runnable> notstarted=new ArrayList<>();
        lock.lock();
        try{
            shutdown=true;
            while(!tasks.isEmpty()){notstarted.add(tasks.poll());};
            isChanged.signalAll();


        }
        finally {

            lock.unlock();
        }
        for(Thread worker : workers){
            worker.interrupt();

        }
        return notstarted;
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline=System.currentTimeMillis()+unit.toMillis(timeout);
        for(Thread worker : workers){
            long left=deadline -System.currentTimeMillis();
            if(left<=0){
                return false;
            }
            worker.join(left);
            if(worker.isAlive()){             // still running after the time ran out
                return false;
            }

        }
        return true;
    }

}
