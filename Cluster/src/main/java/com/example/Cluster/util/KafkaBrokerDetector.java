package com.example.Cluster.util;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

public class KafkaBrokerDetector {

    public static String detectActiveBroker(String ipListCsv, int port, int timeoutMs) {
        List<String> ipList = Arrays.asList(ipListCsv.split(","));

        for (String ip : ipList) {
            String trimmedIp = ip.trim();
            if (isReachable(trimmedIp, port, timeoutMs)) {
                System.out.println("[+] Active broker found: " + trimmedIp + ":" + port);
                return trimmedIp + ":" + port;
            } else {
                System.out.println("[-] Broker not reachable: " + trimmedIp + ":" + port);
            }
        }

        System.out.println("[!] No reachable brokers found.");
        return null;
    }

    private static boolean isReachable(String ip, int port, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(ip, port), timeoutMs);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
