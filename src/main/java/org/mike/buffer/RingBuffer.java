package org.mike.buffer;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RingBuffer<E> {
    private final AtomicReferenceArray<E> buffer;
    private final int capacity;
    private final int mask;

    private long readIdx = 0;
    private long writeIdx = 0;

    private final ReentrantLock putLock = new ReentrantLock();
    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    private final Condition notFull = putLock.newCondition();

    public RingBuffer(int capacityPow2) {
        if (capacityPow2 <= 0 || (capacityPow2 & (capacityPow2 - 1)) != 0) {
            throw new IllegalArgumentException("capacityPow2 must be power of two");
        }
        this.capacity = capacityPow2;
        this.mask = capacityPow2 - 1;
        this.buffer = new AtomicReferenceArray<>(capacityPow2);
    }

    // не блокирующий возвращает true если удалось записать,
    // false если буфер заполнен
    public boolean offer(E element) {
        if (element == null) throw new NullPointerException();
        putLock.lock();
        try {
            //first put
            long currentTail = writeIdx; // 0
            long wrap = currentTail - capacity; // 0-4
            // если заполнен
            if (readIdx <= wrap) {
                return false;
            }
            int idx = (int) (currentTail & mask);
            buffer.set(idx, element);
            writeIdx = currentTail + 1;

            takeLock.lock();
            try {
                notEmpty.signal();
            } finally {
                takeLock.unlock();
            }
            return true;
        } finally {
            putLock.unlock();
        }
    }


    public E poll() {
        takeLock.lock();
        try {
            long currentHead = readIdx;
            if (currentHead >= writeIdx) {
                return null;
            }
            int idx = (int) (currentHead & mask);
            E e = buffer.getAndSet(idx, null);
            readIdx = currentHead + 1;
            // signal producers
            putLock.lock();
            try {
                notFull.signal();
            } finally {
                putLock.unlock();
            }
            return e;
        } finally {
            takeLock.unlock();
        }
    }

    // блокирующий, ожидает, пока не освободится место
    public void put(E element) throws InterruptedException {
        if (element == null) throw new NullPointerException();
        putLock.lockInterruptibly();
        try {
            while (true) {
                long currentTail = writeIdx;
                long wrap = currentTail - capacity;
                if (readIdx > wrap) {
                    int idx = (int) (currentTail & mask);
                    buffer.set(idx, element);
                    writeIdx = currentTail + 1;
                    // signal consumers
                    takeLock.lock();
                    try {
                        notEmpty.signal();
                    } finally {
                        takeLock.unlock();
                    }
                    return;
                }
                notFull.await();
            }
        } finally {
            putLock.unlock();
        }
    }

    // блокирующий - ожидает появления элемента.
    public E take() throws InterruptedException {
        takeLock.lockInterruptibly();
        try {
            while (true) {
                long currentHead = readIdx;
                if (currentHead < writeIdx) {
                    int idx = (int) (currentHead & mask);
                    E e = buffer.getAndSet(idx, null);
                    readIdx = currentHead + 1;
                    // signal producers
                    putLock.lock();

                    try {
                        notFull.signal();
                    } finally {
                        putLock.unlock();
                    }
                    return e;
                }
                notEmpty.await();
            }
        } finally {
            takeLock.unlock();
        }
    }

    public int size() {
        long s = writeIdx - readIdx;
        return s > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) s;
    }

    public String toString() {
        return buffer.toString();
    }
}