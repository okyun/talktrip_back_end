package com.talktrip.talktrip.global.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
public class InstanceConfig {

    @Bean
    public String instanceId() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            return hostname + ":80"; // 포트 80 사용
        } catch (UnknownHostException e) {
            return "unknown:80";
        }
    }
}
