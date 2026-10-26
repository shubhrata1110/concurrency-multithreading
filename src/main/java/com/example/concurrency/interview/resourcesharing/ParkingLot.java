package com.example.concurrency.interview.resourcesharing;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


//with ticket for starvation
//private long nextTicket = 0;      // ticket for the next vehicle that arrives
//private long nowServing = 0;      // only this ticket may park
//
//public int park(int vehicleId, int size) throws InterruptedException {
//    lock.lockInterruptibly();
//    try {
//        long myTicket = nextTicket++;
//        int start;
//        while (myTicket != nowServing || (start = findFree(size)) == -1) {
//            changed.await();
//        }
//        for (int i = start; i < start + size; i++) slots[i] = vehicleId;
//        nowServing++;                  // next in line may try
//        changed.signalAll();
//        return start;
//    } finally {
//        lock.unlock();
//    }
//}
public class ParkingLot {

    int [] slots;
    ReentrantLock lock = new ReentrantLock();
    Condition notFull = lock.newCondition();

    ParkingLot(int size){
        slots = new int[size];

    }

    public int park(int vehicleId,int size) throws InterruptedException{
        lock.lockInterruptibly();
        try{
            int start;
            while((start=findSpace(size))==-1)notFull.await();
            occupy(start,size,vehicleId);
            return start;
        }
        finally {
            lock.unlock();
        }

    }

    public void leave(int vehicleId){
        lock.lock();
        try {

            for (int i = 0; i < slots.length; i++) {
                if (slots[i] == vehicleId) {
                    slots[i] = 0;
                }
            }
            notFull.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    public int findSpace(int size){
        int cnt=0;
        for(int i=0;i<slots.length;i++){

            if(slots[i]==0){
                cnt++;
                if(cnt==size){
                    return i-cnt+1;
                }

            }else{
                cnt=0;
            }

        }
        return -1;
    }
    private void occupy(int start, int size, int vehicleId) {
        for (int i = start; i < start + size; i++) slots[i] = vehicleId;
    }
}
