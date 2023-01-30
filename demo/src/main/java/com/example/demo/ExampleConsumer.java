package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.BiFunction;

public class ExampleConsumer {
    public ExampleConsumer(String topic, BiFunction<String, String, Boolean> biFunction) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:29092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        out: while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(30));
            for (ConsumerRecord<String, String> record : records){
                if(biFunction.apply(record.key(), record.value())){
                    break out;
                }
            }
        }
    }
}