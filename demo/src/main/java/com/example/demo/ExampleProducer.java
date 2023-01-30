package com.example.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class ExampleProducer {

    private final KafkaProducer<String, String> producer;

    ExampleProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String topic, String key, String value) {
        System.out.printf("Sending message topic: %s, key: %s, value: %s%n", topic, key, value);
        producer.send(new ProducerRecord<>(topic, key, value));
    }
}
