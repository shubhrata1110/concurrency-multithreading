package com.example.concurrency.fundamentals.threadpool;
public class ThreadPoolDemo {
    public static void main(String[] args) throws Exception {
        ThreadPool threadpool = new ThreadPool(3,10);

        for(int i=0;i<10;i++){
            int taskNo =i;
            threadpool.execute(()->{
                String message =Thread.currentThread().getName()+": Task "+taskNo;
                System.out.println(message);
            });
        }
        threadpool.waitUntilAllTasksFinished();
        threadpool.stop();
    }
}
