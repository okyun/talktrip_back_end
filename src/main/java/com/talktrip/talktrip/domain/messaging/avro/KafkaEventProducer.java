package com.talktrip.talktrip.domain.messaging.avro;

import com.talktrip.talktrip.domain.messaging.dto.order.OrderCreatedEventDTO;
import com.talktrip.talktrip.domain.messaging.dto.product.ProductClickEventDTO;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Kafka 이벤트 발행자 (JSON 버전)
 *
 * Order와 Product 이벤트를 JSON 형식으로 Kafka에 발행합니다.
 * Spring Kafka의 JsonSerializer를 사용하여 DTO를 그대로 직렬화합니다.
 */
@Component
@RequiredArgsConstructor
public class KafkaEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEventProducer.class);

    /**
     * JSON 직렬화를 사용하는 KafkaTemplate
     *
     * KafkaConfig.jsonKafkaTemplate Bean을 주입받습니다.
     */
    private final KafkaTemplate<String, Object> jsonKafkaTemplate;

    // 주문 생성 이벤트를 발행할 토픽
    @Value("${kafka.topics.order-created:order-created}")
    private String orderCreatedTopic;

    // 상품 클릭 이벤트를 발행할 토픽
    @Value("${kafka.topics.product-click:product-click}")
    private String productClickTopic;

    // ========== Order Event Methods ==========

    /**
     * 주문 생성 이벤트 발행 (JSON 형식)
     *
     * OrderCreatedEventDTO를 그대로 JSON으로 직렬화하여 Kafka로 발행합니다.
     *
     * @param dto OrderCreatedEventDTO
     */
    public void publishOrderCreated(OrderCreatedEventDTO dto) {
        try {
            // send(...) 는 비동기 메서드로, Kafka 브로커에 전송 요청만 하고 바로 Future(CompletableFuture 유사 객체)를 반환합니다.
            // whenComplete(...) 는 이 비동기 작업이 "성공 또는 실패로 끝난 이후"에 호출되는 콜백을 등록합니다.
            //  - result: 성공 시 RecordMetadata 를 포함 (어느 토픽/파티션/오프셋에 적혔는지)
            //  - ex: 실패 시 예외 정보 (null 이 아니면 전송 실패)
            jsonKafkaTemplate.send(orderCreatedTopic, String.valueOf(dto.getOrderId()), dto)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            // ✅ 전송 성공: 메타데이터를 로깅
                            logger.info("JSON 주문 생성 이벤트 발행 성공: orderId={}, orderCode={}, topic={}, partition={}, offset={}",
                                    dto.getOrderId(), dto.getOrderCode(),
                                    result.getRecordMetadata().topic(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            // ❌ 전송 실패: 예외 정보를 함께 로깅 (추후 모니터링/알람 용도)
                            logger.error("JSON 주문 생성 이벤트 발행 실패: orderId={}, orderCode={}",
                                    dto.getOrderId(), dto.getOrderCode(), ex);
                        }
                    });
        } catch (Exception e) {
            logger.error("JSON 주문 생성 이벤트 발행 중 오류 발생: orderId={}, orderCode={}",
                    dto.getOrderId(), dto.getOrderCode(), e);
            throw new RuntimeException("JSON 주문 생성 이벤트 발행 실패", e);
        }
    }

    /**
     * 주문 생성 이벤트 발행 (JSON 형식) - 별칭 메서드
     *
     * @param dto OrderCreatedEventDTO
     */
    public void publishOrderEvent(OrderCreatedEventDTO dto) {
        publishOrderCreated(dto);
    }

    // ========== Product Event Methods ==========

    /**
     * 상품 클릭 이벤트 발행 (JSON 형식)
     *
     * @param productId 상품 ID
     */
    public void publishProductClick(Long productId) {
        publishProductClick(productId, null);
    }

    /**
     * 상품 클릭 이벤트 발행 (JSON 형식) - String productId
     *
     * @param productId 상품 ID (String)
     */
    public void publishProductClick(String productId) {
        try {
            Long productIdLong = Long.parseLong(productId);
            publishProductClick(productIdLong, null);
        } catch (NumberFormatException e) {
            logger.error("상품 ID 파싱 실패: productId={}", productId, e);
            throw new IllegalArgumentException("유효하지 않은 상품 ID: " + productId, e);
        }
    }

    /**
     * 상품 클릭 이벤트 발행 (JSON 형식) - memberId 포함
     *
     * @param productId 상품 ID
     * @param memberId  회원 ID (null 가능)
     */
    public void publishProductClick(Long productId, Long memberId) {
        try {
            ProductClickEventDTO event = ProductClickEventDTO.of(productId, memberId);

            // 여기서도 마찬가지로 send(...) 는 비동기 호출이고,
            // whenComplete(...) 콜백 안에서 성공/실패 여부에 따라 로그만 남기고 있습니다.
            // 비즈니스 로직(트랜잭션 커밋 여부 등)은 send() 호출 이전에 이미 끝난 상태여야 안전합니다.
            jsonKafkaTemplate.send(productClickTopic, String.valueOf(productId), event)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            logger.info("JSON 상품 클릭 이벤트 발행 성공: productId={}, memberId={}, topic={}, partition={}, offset={}",
                                    productId, memberId,
                                    result.getRecordMetadata().topic(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            logger.error("JSON 상품 클릭 이벤트 발행 실패: productId={}, memberId={}",
                                    productId, memberId, ex);
                        }
                    });
        } catch (Exception e) {
            logger.error("JSON 상품 클릭 이벤트 발행 중 오류 발생: productId={}, memberId={}",
                    productId, memberId, e);
            throw new RuntimeException("JSON 상품 클릭 이벤트 발행 실패", e);
        }
    }

    /**
     * 주문 이벤트 발행 (JSON 형식) - 직접 파라미터로 받기
     *
     * orderId, customerId, quantity, price를 직접 받아서 간단한 OrderCreatedEventDTO를 구성 후 발행합니다.
     *
     * @param orderId    주문 ID (UUID 문자열 등)
     * @param customerId 고객 ID (숫자 문자열일 경우 memberId로 사용 시도)
     * @param quantity   주문 수량
     * @param price      주문 가격 (BigDecimal)
     */
    public void publishOrderEvent(String orderId, String customerId, Integer quantity, BigDecimal price) {
        try {
            Long memberIdLong = null;
            if (customerId != null && !customerId.isEmpty()) {
                try {
                    memberIdLong = Long.parseLong(customerId);
                } catch (NumberFormatException e) {
                    logger.warn("customerId를 Long으로 변환할 수 없습니다. null로 설정: customerId={}", customerId);
                }
            }

            OrderCreatedEventDTO dto = OrderCreatedEventDTO.builder()
                    .orderId(0L) // 간단한 테스트용이므로 0L 사용 (실제 프로덕션에서는 별도 처리 필요)
                    .orderCode(orderId)
                    .memberId(memberIdLong)
                    .totalPrice(price != null ? price.intValue() : 0)
                    .orderStatus("PENDING")
                    .createdAt(LocalDateTime.now())
                    .items(List.of())
                    .build();

            publishOrderCreated(dto);
            logger.info("JSON 주문 이벤트 발행 성공 (직접 파라미터): orderId={}, customerId={}, quantity={}, price={}",
                    orderId, customerId, quantity, price);
        } catch (Exception e) {
            logger.error("JSON 주문 이벤트 발행 실패 (직접 파라미터): orderId={}, customerId={}",
                    orderId, customerId, e);
            throw new RuntimeException("JSON 주문 이벤트 발행 실패", e);
        }
    }

    /**
     * 상품 이벤트 발행 (JSON 형식) - 직접 파라미터로 받기
     *
     * @param productId 상품 ID
     * @param memberId  회원 ID (nullable)
     */
    public void publishProductEvent(Long productId, Long memberId) {
        publishProductClick(productId, memberId);
        logger.info("JSON 상품 이벤트 발행 성공 (직접 파라미터): productId={}, memberId={}", productId, memberId);
    }
}


