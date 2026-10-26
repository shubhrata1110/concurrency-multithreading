package com.example.concurrency.interview.scheduling.jobscheduler;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class JobScheduler {

    private final TaskSource source;

    private Deque<Task> readyTasks = new ArrayDeque<>();
    private Set<String> seenIds= new HashSet<>();
    private List<Thread>workers=new ArrayList<>();
    private Set<String> finishedIds= new HashSet<>();
    private Exception error;
    private int runningCount=0;

    ReentrantLock lock = new ReentrantLock();
    Condition isChanged = lock.newCondition();

    //List<Task> getNextTasks(Set<String> finishedTaskIds);

    public JobScheduler(TaskSource source){

        this.source = source;
    }

    public void runAll(int maxWorkers) throws Exception{
        lock.lockInterruptibly();
        try{
            addToReadyQueue();
        }
        finally{
            lock.unlock();
        }

        for(int i=0;i<maxWorkers;i++){
            Thread thread =new Thread(this::workerLoop);
            workers.add(thread);
            thread.start();
        }

        for (Thread worker : workers) {
            worker.join();                             // wait until everything is done
        }
        if (error != null) throw error;



    }

    private void workerLoop(){
        Task task;
        while(true){
            try{
                task= getReadyTask();
            }
            catch(InterruptedException e){
                return;
            }

            if(task==null)return;
            try{
                task.doWork();
                finish(task, null);
            }
            catch(Exception e){
                finish(task, e);
            }


        }
    }

    private void finish(Task task, Exception e){
        lock.lock();
        try{
            runningCount--;
            if(e!=null){
                if(error==null)error=e;            // keep the first error
            }
            else {
                finishedIds.add(task.taskID());    // mark done FIRST, so its dependents become ready
                addToReadyQueue();
            }
            isChanged.signalAll();

        }
        finally{
            lock.unlock();
        }
    }

    private Task getReadyTask() throws InterruptedException{
        lock.lockInterruptibly();
        try{
            while(error==null && readyTasks.isEmpty() && runningCount>0)isChanged.await();
            if(error!=null || readyTasks.isEmpty())return null;
            runningCount++;
            return readyTasks.poll();

        }
        finally{
            lock.unlock();
        }
    }

    // called with the lock held
    private void addToReadyQueue(){
        for(Task t  : source.getNextTasks(finishedIds)){
            if(!seenIds.contains(t.taskID())){
                seenIds.add(t.taskID());
                readyTasks.add(t);
                isChanged.signalAll();
            }

        }
    }








}
