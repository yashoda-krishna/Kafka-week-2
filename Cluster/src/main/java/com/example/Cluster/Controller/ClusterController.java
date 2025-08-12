package com.example.Cluster.Controller;

import com.example.Cluster.Service.Consumer;
import com.example.Cluster.Service.Producer;
import com.example.Cluster.util.KafkaBrokerDetector;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class ClusterController {

    private final Producer producerService;
    private final Consumer consumerService;

    @Value("${kafka.broker.ips}")
    private String brokerIps;

    @Value("${kafka.broker.port}")
    private int brokerPort;

    @Value("${kafka.broker.timeout}")
    private int timeoutMs;

    public ClusterController(Producer producerService, Consumer consumerService) {
        this.producerService = producerService;
        this.consumerService = consumerService;
    }

    @GetMapping("/send")
    public String send() {
        String activeBroker = KafkaBrokerDetector.detectActiveBroker(brokerIps, brokerPort, timeoutMs);
        if (activeBroker == null) {
            return "[!] No Kafka broker is currently reachable.";
        }

        producerService.sendMessages(activeBroker);
        return "Messages sent to broker: " + activeBroker;
    }

    @GetMapping("/send-and-consume")
    public String sendAndConsume() {
        String activeBroker = KafkaBrokerDetector.detectActiveBroker(brokerIps, brokerPort, timeoutMs);
        if (activeBroker == null) {
            return "[!] No Kafka broker is currently reachable.";
        }

        producerService.sendMessages(activeBroker);
        consumerService.consumeMessages(activeBroker);
        return "Sent and consumed messages via broker: " + activeBroker;
    }
}
