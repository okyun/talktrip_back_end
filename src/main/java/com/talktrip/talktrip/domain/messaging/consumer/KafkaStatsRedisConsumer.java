package com.talktrip.talktrip.domain.messaging.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka Streams 집계 결과를 Redis에 반영하는 Consumer
 *
 * 패턴 A)
 * - Streams: product-click → product-click-stats / order-created → order-purchase-stats
 * - Listener: product-click-stats, order-purchase-stats → Redis에 TOP30 저장
 *
 * API 레이어에서는 Redis만 조회하면 되도록 구성합니다.
 */
@Component
@RequiredArgsConstructor
public class KafkaStatsRedisConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStatsRedisConsumer.class);

    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;

    /**
     * 상품 클릭 통계(product-click-stats) → Redis 저장
     *
     * - key(카프카): windowStartTime(ms) 문자열
     * - value(카프카): List<ProductClickStatResponse> (JSON)
     *
     * Redis 저장 키 설계:
     * - 최신 윈도우 전체: stats:product-click:latest
     * - 윈도우별 저장:   stats:product-click:{windowStart}
     *
     * 멱등성:
     * - 동일 windowStart 에 대해 항상 "마지막 값으로 overwrite" 하므로, 재시도/중복 수신에도 안전합니다.
     */
    @KafkaListener(
            topics = "${kafka.topics.product-click-stats:product-click-stats}",
            groupId = "product-click-stats-redis-consumer",
            concurrency = "1",
            containerFactory = "genericKafkaListenerContainerFactory"
    )
    public void updateProductClickStats(
            @Payload Object payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String windowStartKey
    ) {
        try {
            String json = objectMapper.writeValueAsString(payload);

            // 최신 TOP30 저장
            stringRedisTemplate.opsForValue().set("stats:product-click:latest", json);
            // 윈도우별 TOP30 저장
            if (windowStartKey != null) {
                stringRedisTemplate.opsForValue().set("stats:product-click:" + windowStartKey, json);
            }

            logger.info("[Stats][Redis] product-click-stats 반영 완료: windowStartKey={}, length={}",
                    windowStartKey, json.length());
        } catch (JsonProcessingException e) {
            logger.error("[Stats][Redis] product-click-stats 직렬화 실패: windowStartKey={}, error={}",
                    windowStartKey, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("[Stats][Redis] product-click-stats Redis 반영 실패: windowStartKey={}, error={}",
                    windowStartKey, e.getMessage(), e);
        }
    }

    /**
     * 주문 구매 통계(order-purchase-stats) → Redis 저장
     *
     * - key(카프카): windowStartTime(ms) 문자열
     * - value(카프카): List<OrderPurchaseStatResponse> (JSON)
     *
     * Redis 저장 키 설계:
     * - 최신 윈도우 전체: stats:order-purchase:latest
     * - 윈도우별 저장:   stats:order-purchase:{windowStart}
     */
    @KafkaListener(
            topics = "${kafka.topics.order-purchase-stats:order-purchase-stats}",
            groupId = "order-purchase-stats-redis-consumer",
            concurrency = "1",
            containerFactory = "genericKafkaListenerContainerFactory"
    )
    public void updateOrderPurchaseStats(
            @Payload Object payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String windowStartKey
    ) {
        try {
            String json = objectMapper.writeValueAsString(payload);

            stringRedisTemplate.opsForValue().set("stats:order-purchase:latest", json);
            if (windowStartKey != null) {
                stringRedisTemplate.opsForValue().set("stats:order-purchase:" + windowStartKey, json);
            }

            logger.info("[Stats][Redis] order-purchase-stats 반영 완료: windowStartKey={}, length={}",
                    windowStartKey, json.length());
        } catch (JsonProcessingException e) {
            logger.error("[Stats][Redis] order-purchase-stats 직렬화 실패: windowStartKey={}, error={}",
                    windowStartKey, e.getMessage(), e);
        } catch (Exception e) {
            logger.error("[Stats][Redis] order-purchase-stats Redis 반영 실패: windowStartKey={}, error={}",
                    windowStartKey, e.getMessage(), e);
        }
    }
}


