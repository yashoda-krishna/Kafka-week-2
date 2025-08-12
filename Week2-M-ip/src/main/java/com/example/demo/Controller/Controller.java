package com.example.demo.Controller;



import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.Properties;

@RestController
@RequestMapping("/api/kafka")
public class Controller {

    @GetMapping("/createTopic")
    public String createTopicInMultipleClusters() {
        String clusters = "10.0.4.85:9092,10.0.4.214:9092,10.0.4.221:9092,10.0.7.202:9092";
        String[] clusterArray = clusters.split(",");
        String topicName = "Jaya";
        int partitions = 4;
        short replicationFactor = 1;

        StringBuilder response = new StringBuilder();

        for (String cluster : clusterArray) {
            Properties config = new Properties();
            config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster);

            try (AdminClient adminClient = AdminClient.create(config)) {
                NewTopic topic = new NewTopic(topicName, partitions, replicationFactor);
                adminClient.createTopics(Collections.singleton(topic)).all().get();
                String successMsg = "Topic created in cluster: " + cluster + "<br>";
                System.out.println(successMsg);
                response.append(successMsg);
            } catch (Exception e) {
                String errorMsg = "Failed to create topic in cluster: " + cluster + " - " + e.getMessage() + "<br>";
                System.err.println(errorMsg);
                response.append(errorMsg);
            }
        }
        return response.toString();
    }
}
