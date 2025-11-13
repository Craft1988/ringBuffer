package org.mike.buffer;

import java.util.Arrays;
import java.util.Optional;

public class OverwritingRingBuffer {
    private final Object[] buffer;
    private final int capacity;
    private int writeIndex = 0;
    private int readIndex = 0;
    private int count = 0;

    public OverwritingRingBuffer(int capacity) {
        this.capacity = capacity;
        this.buffer = new Object[capacity];
    }

    public synchronized boolean put(Object value) {
        if (isFull()) {
            readIndex = (readIndex + 1) % capacity;
            count--;
        }
        buffer[writeIndex] = value;
        writeIndex = (writeIndex + 1) % capacity;
        count++;
        return true;
    }

    public synchronized Optional<Object> poll() {
        if (isEmpty()) {
            return Optional.empty();
        }
        Object value = buffer[readIndex];
        readIndex = (readIndex + 1) % capacity;
        count--;
        return Optional.of(value);
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public boolean isFull() {
        return count == capacity;
    }

    public int size() {
        return count;
    }

    public String toString() {
        return Arrays.toString(buffer);
    }

}