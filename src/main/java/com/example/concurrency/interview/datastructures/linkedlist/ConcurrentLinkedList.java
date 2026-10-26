package com.example.concurrency.interview.datastructures.linkedlist;
public class ConcurrentLinkedList {

    private static class Node{
        final int value;
        volatile Node next;
        Node(int value){
            this.value = value;
        }
    }
    private volatile Node head;
    private  Node tail;

    private final SearchInsertDeleteLock sync = new SearchInsertDeleteLock();   // one per list, not static


    public boolean search(int value) throws InterruptedException {
        sync.startSearch();
        try{
            for(Node cur = head; cur != null; cur = cur.next){
                if(cur.value==value){
                    return true;
                }
            }
            return false;
        }
        finally {
            sync.stopSearch();
        }


    }
    public void insert(int value) throws InterruptedException {
        sync.startInsert();
        try{
            Node newNode = new Node(value);

            if(head==null){
                head = newNode;
            }
            else{
                tail.next = newNode;
            }
            tail = newNode;
        }
        finally {
            sync.stopInsert();
        }

    }

    public boolean delete(int value) throws InterruptedException {
        sync.startDelete();
        try{
            Node prev=null;
            for(Node cur=head; cur!=null; cur=cur.next){
                if(cur.value==value){
                    if(prev==null){head=cur.next;}
                    else prev.next=cur.next;
                    if(cur==tail){tail=prev;}      // deleted the last node: prev is the new tail
                    return true;
                }
                prev=cur;
            }
            return false;
        }
        finally {
            sync.stopDelete();
        }
    }



}
