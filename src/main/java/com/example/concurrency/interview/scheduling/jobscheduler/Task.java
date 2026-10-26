package com.example.concurrency.interview.scheduling.jobscheduler;
public interface Task {
    String taskID();
    void doWork() throws Exception;



}
