package ru.romanov.producer.producer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@EnableScheduling
@Slf4j
public class ProducerService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Value("${spring.kafka.topic.name}")
    private String topicName;

    private static int counter = 0;

    @Scheduled(fixedRate = 200)
    @SneakyThrows
    public void sendMessage(){
        kafkaTemplate.send(topicName, "Hello from producer: " + counter++);
        log.info("Сообщение отправленно: " + counter);
    }
}
