package com.example.demo.Controller;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/kafka")
public class KafkaMessageSender {

    private static final String CLUSTERS = "10.0.4.85:9092,10.0.4.214:9092,10.0.4.221:9092,10.0.7.202:9092";
    private static final String TOPIC_NAME = "Jaya";
    private static final int PARTITIONS = 4;
    private static final short REPLICATION_FACTOR = 1;

    @GetMapping("/send")
    public String sendMessageToAllClusters(@RequestParam String message) {
        StringBuilder response = new StringBuilder();

        for (String cluster : CLUSTERS.split(",")) {
            try {
                ensureTopicExists(cluster, TOPIC_NAME);
                sendMessage(cluster, TOPIC_NAME, message);
                append(response, "✅ Sent message to cluster: " + cluster);
            } catch (Exception e) {
                append(response, "❌ Failed on cluster: " + cluster + " - " + e.getMessage());
            }
        }

        return response.toString();
    }

    private void ensureTopicExists(String cluster, String topicName) throws Exception {
        Properties adminConfig = new Properties();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster);

        try (AdminClient adminClient = AdminClient.create(adminConfig)) {
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                NewTopic topic = new NewTopic(topicName, PARTITIONS, REPLICATION_FACTOR);
                adminClient.createTopics(Collections.singleton(topic)).all().get();
                System.out.println("📌 Created topic '" + topicName + "' in cluster: " + cluster);
            } else {
                System.out.println("ℹ️ Topic already exists in cluster: " + cluster);
            }
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) throw e;
        }
    }

    private void sendMessage(String cluster, String topicName, String message) throws Exception {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig)) {
            producer.send(new ProducerRecord<>(topicName, message)).get(); // synchronous send
        }
    }

    private void append(StringBuilder sb, String msg) {
        System.out.println(msg);
        sb.append(msg).append("<br>");
    }
}
