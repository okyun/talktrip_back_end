package com.talktrip.talktrip.global.redis;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "redis.retry")
public class RedisRetryProperties {
    private int maxAttempts = 3;
    private long backoffInitial = 1000;
    private long backoffMax = 10000;
    private double backoffMultiplier = 2.0;
}