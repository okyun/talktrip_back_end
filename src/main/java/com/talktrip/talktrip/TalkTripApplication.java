package com.talktrip.talktrip;

import com.talktrip.talktrip.global.config.SeoulDateTimeProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.auditing.DateTimeProvider;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableJpaAuditing(dateTimeProviderRef = "seoulDateTimeProvider")
@EnableAsync
public class TalkTripApplication {

    public static void main(String[] args) {
        SpringApplication.run(TalkTripApplication.class, args);
    }

    @Bean
    public DateTimeProvider seoulDateTimeProvider() {
        return new SeoulDateTimeProvider();
    }

}
