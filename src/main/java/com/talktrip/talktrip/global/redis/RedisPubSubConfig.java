package com.talktrip.talktrip.global.redis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.util.ErrorHandler;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Redis Pub/Sub 설정 클래스
 * 
 * Redis의 Publish/Subscribe 기능을 사용하여 여러 서버 인스턴스 간에
 * 실시간 메시지를 주고받을 수 있도록 설정합니다.
 * 
 * 주요 기능:
 * 1. Redis 메시지 리스너 컨테이너 설정
 * 2. 메시지 처리용 스레드 풀 설정
 * 3. 오류 처리 핸들러 설정
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class RedisPubSubConfig {

    /**
     * Redis 메시지 처리를 위한 전용 스레드 풀 생성
     * 
     * newCachedThreadPool의 특징:
     * - 필요에 따라 스레드를 생성하고 재사용
     * - 60초 동안 사용되지 않은 스레드는 자동으로 제거
     * - 높은 처리량과 메모리 효율성 제공
     * 
     * @return Redis 메시지 처리용 Executor
     */
    @Bean
    public Executor redisMessageExecutor() {
        return Executors.newCachedThreadPool(new ThreadFactory() {
            // 스레드 번호를 안전하게 증가시키는 카운터
            private final AtomicInteger threadNumber = new AtomicInteger(1);
            
            @Override
            public Thread newThread(Runnable r) {
                // 스레드 이름: redis-message-1, redis-message-2, ...
                Thread thread = new Thread(r, "redis-message-" + threadNumber.getAndIncrement());
                
                // 데몬 스레드로 설정 (애플리케이션 종료 시 자동으로 정리됨)
                thread.setDaemon(true);
                
                // 일반 우선순위 설정 (1-10 중 5)
                thread.setPriority(Thread.NORM_PRIORITY);
                
                return thread;
            }
        });
    }

    /**
     * Redis 메시지 처리 중 발생하는 오류를 처리하는 핸들러
     * 
     * 주요 처리 오류:
     * 1. Redis 연결 실패
     * 2. JSON 직렬화/역직렬화 오류
     * 3. 기타 예외 상황
     * 
     * @return Redis 오류 처리용 ErrorHandler
     */
    @Bean
    public ErrorHandler redisErrorHandler() {
        return new ErrorHandler() {
            @Override
            public void handleError(Throwable t) {
                // 기본 오류 로그 출력
                log.error("[RedisPubSub] Redis 메시지 처리 중 오류 발생: {}", t.getMessage(), t);
                
                // Redis 연결 오류인 경우 재연결 시도 로그
                if (t instanceof org.springframework.data.redis.RedisConnectionFailureException) {
                    log.warn("[RedisPubSub] Redis 연결 오류 감지, 재연결 시도 중...");
                }
                
                // 메시지 직렬화 오류인 경우 (JSON 파싱 실패 등)
                if (t instanceof com.fasterxml.jackson.core.JsonProcessingException) {
                    log.error("[RedisPubSub] JSON 직렬화/역직렬화 오류: {}", t.getMessage());
                }
            }
        };
    }

    /**
     * Redis 메시지 리스너 컨테이너 설정
     * 
     * 이 컨테이너는 Redis의 Pub/Sub 기능을 사용하여:
     * 1. 지정된 패턴의 메시지를 수신
     * 2. 별도 스레드 풀에서 메시지 처리
     * 3. 오류 발생 시 적절한 처리
     * 
     * @param connectionFactory Redis 연결 팩토리
     * @param subscriber Redis 메시지를 처리할 구독자 (RedisSubscriber)
     * @param redisErrorHandler 오류 처리 핸들러
     * @param redisMessageExecutor 메시지 처리용 스레드 풀
     * @return 설정된 RedisMessageListenerContainer
     */
    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(
            //redisMessageListenerContainer - 데몬 스레드 , 서버가 종료되어도 스레드가 자원 받는것을 원활하게 할 수 있다.
            RedisConnectionFactory connectionFactory,
            ErrorHandler redisErrorHandler,
            Executor redisMessageExecutor
    ) {
        log.info("[RedisPubSubConfig] Redis 메시지 리스너 컨테이너 설정 시작");
        
        // Redis 메시지 리스너 컨테이너 생성
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        
        // Redis 연결 팩토리 설정
        container.setConnectionFactory(connectionFactory);
        
        // 오류 처리 핸들러 설정 (예외 발생 시 로깅 및 처리)
        container.setErrorHandler(redisErrorHandler);
        
        // 메시지 처리를 위한 스레드 풀 설정 (병렬 처리로 성능 향상)
        container.setTaskExecutor(redisMessageExecutor);

        log.info("[RedisPubSubConfig] Redis 메시지 리스너 컨테이너 설정 완료");
        return container;
    }

    /**
     * Redis Pub/Sub 구독 관리 유틸리티 클래스
     * 
     * 개별 채널 구독/해제 및 패턴 구독 관리를 위한 편의 메서드들을 제공합니다.
     */
    public static class RedisSubscriptionManager {
        
        private final RedisMessageListenerContainer container;
        private final MessageListener subscriber;
        
        public RedisSubscriptionManager(RedisMessageListenerContainer container, 
                                      MessageListener subscriber) {
            this.container = container;
            this.subscriber = subscriber;
        }
        

        /**
         * 특정 채팅방 채널에서 구독 해제
         * 
         * @param roomId 채팅방 ID
         */
        public void unsubscribeFromRoomChannel(String roomId) {
            try {
                String channel = "chat:room:" + roomId;
                container.removeMessageListener(subscriber, new ChannelTopic(channel));
                log.info("[RedisSubscriptionManager] 채팅방 채널 구독 해제: {}", channel);
            } catch (Exception e) {
                log.error("[RedisSubscriptionManager] 채팅방 채널 구독 해제 실패: roomId={}, error={}", 
                        roomId, e.getMessage(), e);
            }
        }
        

        
        /**
         * 특정 사용자 채널에서 구독 해제
         * 
         * @param userId 사용자 ID
         */
        public void unsubscribeFromUserChannel(String userId) {
            try {
                String channel = "chat:user:" + userId;
                container.removeMessageListener(subscriber, new ChannelTopic(channel));
                log.info("[RedisSubscriptionManager] 사용자 채널 구독 해제: {}", channel);
            } catch (Exception e) {
                log.error("[RedisSubscriptionManager] 사용자 채널 구독 해제 실패: userId={}, error={}", 
                        userId, e.getMessage(), e);
            }
        }
        

        /**
         * 패턴 기반 구독 해제
         * 
         * @param pattern 해제할 패턴 (예: "chat:room:*", "chat:user:*")
         */
        public void unsubscribeFromPattern(String pattern) {
            try {
                container.removeMessageListener(subscriber, new PatternTopic(pattern));
                log.info("[RedisSubscriptionManager] 패턴 구독 해제: {}", pattern);
            } catch (Exception e) {
                log.error("[RedisSubscriptionManager] 패턴 구독 해제 실패: pattern={}, error={}", 
                        pattern, e.getMessage(), e);
            }
        }
        
        /**
         * 모든 구독 해제 (애플리케이션 종료 시 사용)
         */
        public void unsubscribeAll() {
            try {
                log.info("[RedisSubscriptionManager] 모든 구독 해제 시작");
                
                // 패턴 기반 구독 해제
                unsubscribeFromPattern("chat:room:*");
                unsubscribeFromPattern("chat:user:*");
                
                log.info("[RedisSubscriptionManager] 모든 구독 해제 완료");
            } catch (Exception e) {
                log.error("[RedisSubscriptionManager] 모든 구독 해제 실패: error={}", e.getMessage(), e);
            }
        }
        

    }
}
