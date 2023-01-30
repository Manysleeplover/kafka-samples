package com.example.demo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaExampleMain {

    public static void main(String[] args) throws InterruptedException {
        final String topic = "my-test-topic";
        int count = 5;
        AtomicInteger atomicInteger = new AtomicInteger(0);

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        //start consumer
        executorService.execute(() -> {
            new ExampleConsumer(topic, (key, value) -> {
                System.out.printf("consumed: key = %s, value = %s%n", key, value);
                return atomicInteger.incrementAndGet() == count;
            });
        });

        //start producer
        executorService.execute(() -> {
            ExampleProducer exampleProducer = new ExampleProducer();
            for (int i = 0; i < count; i++) {
                exampleProducer.sendMessage(topic, Integer.toString(i), "message - " + i);
            }
        });

        executorService.shutdown();
        executorService.awaitTermination(3, TimeUnit.MINUTES);
    }
}