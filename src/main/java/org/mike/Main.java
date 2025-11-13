package org.mike;

import org.mike.buffer.OverwritingRingBuffer;

public class Main {
    public static void main(String[] args) {
        OverwritingRingBuffer ringBuffer = new OverwritingRingBuffer(3);
        ringBuffer.put(1);
        System.out.println(ringBuffer);
        ringBuffer.put(2);
        ringBuffer.put(3);
        System.out.println(ringBuffer);
        ringBuffer.put(4);
        System.out.println(ringBuffer);
        ringBuffer.poll();
        ringBuffer.poll();

        ringBuffer.put(7);
        ringBuffer.put(8);
    }
}
