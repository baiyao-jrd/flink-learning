package com.atguigu.day09;

import java.util.ArrayList;

public class Example8 {
    public static void main(String[] args) {
        Queue queue = new Queue(2);
        queue.inQueue(1);
        queue.inQueue(2);
        queue.deQueue();
        queue.inQueue(3);
    }

    public static class Queue {
        public int size;
        public ArrayList<Integer> queue;

        public Queue(int size) {
            this.size = size;
            this.queue = new ArrayList<>();
        }

        public void inQueue(Integer i) {
            if (queue.size() == size) {
                throw new RuntimeException("队列已满");
            }
            queue.add(i);
        }

        public Integer deQueue() {
            if (queue.size() == 0) {
                throw new RuntimeException("队列为空");
            }
            Integer result = queue.get(0);
            queue.remove(0);
            return result;
        }
    }
}
