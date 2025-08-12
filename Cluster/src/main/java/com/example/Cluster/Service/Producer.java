package com.example.Cluster.Service;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service
public class Producer {

    private static final String TOPIC = "test-topic";

    public void sendMessages(String activeBroker) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, activeBroker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 5; i++) {
                String msg = "Hello Kafka " + i;
                producer.send(new ProducerRecord<>(TOPIC, msg));
                System.out.println("[>] Sent: " + msg);
            }
            producer.flush();
        }
    }
}
