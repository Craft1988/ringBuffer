package org.mike;

import org.mike.buffer.RingBuffer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) {
        RingBuffer<Integer> buffer = new RingBuffer<>(4);


        ExecutorService executor = Executors.newFixedThreadPool(4);

        // Продюсеры
        var producers = IntStream.range(0, 2)
                .mapToObj(id -> CompletableFuture.runAsync(() -> {
                    try {
                        for (int i = 0; i < 4; i++) {
                            buffer.put(i + id * 100);
                            System.out.println("Producer-" + id + " produced: " + (i + id * 100));
                            Thread.sleep(100);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, executor))
                .toList();

        // Консьюмеры
        var consumers = IntStream.range(0, 2)
                .mapToObj(id -> CompletableFuture.runAsync(() -> {
                    try {
                        for (int i = 0; i < 3; i++) {
                            Integer value = buffer.take();
                            System.out.println("Consumer-" + id + " consumed: " + value);
                            Thread.sleep(150);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, executor))
                .toList();

        // Ожидание завершения всех задач
        CompletableFuture<Void> all = CompletableFuture.allOf(
                Stream.concat(producers.stream(), consumers.stream())
                        .toArray(CompletableFuture[]::new)
        );

        all.join(); // блокируемся до завершения всех

        executor.shutdown();
    }
}