package com.talktrip.talktrip.global.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Spring {@code @Scheduled} 활성화를 위한 설정.
 * <p>
 * {@link EnableScheduling} 만으로 스케줄링 인프라가 등록되며, 애플리케이션 내 다른 빈에 붙은
 * {@code @Scheduled} 메서드가 실행 대상이 됩니다. 별도 {@code @Bean} 이나 메서드가 없어도 됩니다.
 * <p>
 * 스레드 풀 크기 조정 등이 필요하면 이후 {@link org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler}
 * 등을 {@code @Bean} 으로 추가할 수 있습니다.
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
}

