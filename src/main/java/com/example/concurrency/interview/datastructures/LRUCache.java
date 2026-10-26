package com.example.concurrency.interview.datastructures;
import java.util.*;
import java.util.concurrent.locks.*;


public class LRUCache <K,V>{
    private class Node{
        private K key;
        private V value;
        private Node next;
        private Node prev;

        public Node(K key,V value){
            this.key = key;
            this.value = value;

        }
    }

    private Node head= new Node(null,null);
    private Node tail=new Node(null,null);
    private int capacity;

    private Map<K,Node> map=new HashMap<>();

    private final ReentrantLock lock =new ReentrantLock();

    public LRUCache(int capacity){
        if(capacity<=0) throw new IllegalArgumentException("capacity must be > 0");
        this.capacity=capacity;
        head.next=tail;     // empty list: the two sentinels point at each other
        tail.prev=head;
    }

    public V get(K key){
        lock.lock();
        try{
            Node node=map.get(key);
            if(node==null){return null;}
            moveToFront(node);
            return node.value;

        }
        finally {
            lock.unlock();
        }
    }
    public void put(K key,V value){
        lock.lock();
        try{
            Node node =map.get(key);
            if(node!=null){
                node.value=value;
                moveToFront(node);
                return;

            }
            if(map.size()==capacity){
                Node lruNode=tail.prev;
                removeFromCache(lruNode);
                map.remove(lruNode.key);

            }
            Node newNode= new Node(key, value);
            addInFront(newNode);
            map.put(key,newNode);


        }
        finally {
            lock.unlock();

        }
    }
    private void moveToFront(Node node){
        removeFromCache(node);
        addInFront(node);
    }
    private void removeFromCache(Node node){

        node.prev.next = node.next;
        node.next.prev = node.prev;

    }
    private void addInFront(Node node){
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }
}
