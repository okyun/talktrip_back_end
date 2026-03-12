package com.talktrip.talktrip.domain.messaging.consumer;

import com.talktrip.talktrip.domain.member.repository.MemberRepository;
import com.talktrip.talktrip.domain.product.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Kafka 이벤트 디버깅용 Consumer
 *
 * - 토픽이 정상적으로 발행/소비되는지 확인하기 위한 가벼운 테스트/로그용 Consumer 입니다.
 * - 비즈니스 로직은 수행하지 않고, 수신한 메시지를 로그로만 출력합니다.
 *
 * ⚠ 주의:
 * - Kafka Streams Processor(ProductClickProcessor, OrderPurchaseProcessor)와는 별개로 동작합니다.
 * - groupId 를 별도로 설정해서 기존 컨슈머 그룹과 충돌하지 않도록 했습니다.
 */
@Component
@RequiredArgsConstructor
public class KafkaEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEventConsumer.class);

    private final MemberRepository memberRepository;
    private final ProductRepository productRepository;

    /**
     * product-click 토픽 디버깅용 Consumer
     *
     * - 값(value)은 JsonDeserializer에 의해 Map 형태로 역직렬화됩니다.
     * - 토픽/파티션/오프셋/키와 함께 payload 전체를 로그로 남깁니다.
     * - concurrency: 동시에 몇 개의 Consumer 스레드를 띄울지 지정 (여기서는 1로 명시)
     * - containerFactory: 어떤 Listener Container 설정을 사용할지 지정
     *   (KafkaConfig.genericKafkaListenerContainerFactory 사용 → value를 Object/Map 등으로 처리)
     */
    @KafkaListener(
            topics = "${kafka.topics.product-click:product-click}",
            groupId = "debug-product-click-consumer",
            concurrency = "1",
            containerFactory = "genericKafkaListenerContainerFactory"
    )
    public void listenProductClick(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        Long productId = numberFromPayload(payload, "productId");
        Long memberId = numberFromPayload(payload, "memberId");
        String userLabel = resolveUserLabel(memberId);
        String productLabel = resolveProductLabel(productId);
        logger.info("{} 사용자가 {} 상품을 클릭했습니다. topic={}, partition={}, offset={}",
                userLabel, productLabel, topic, partition, offset);
    }

    private Long numberFromPayload(Map<String, Object> payload, String key) {
        if (payload == null) return null;
        Object v = payload.get(key);
        if (v == null) return null;
        if (v instanceof Number n) return n.longValue();
        try {
            return Long.parseLong(v.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String resolveUserLabel(Long memberId) {
        if (memberId == null) return "비회원";
        return memberRepository.findById(memberId)
                .map(m -> {
                    String n = m.getNickname();
                    if (n != null && !n.isBlank()) return n;
                    String name = m.getName();
                    return (name != null && !name.isBlank()) ? name : "회원(id:" + memberId + ")";
                })
                .orElse("회원(id:" + memberId + ")");
    }

    private String resolveProductLabel(Long productId) {
        if (productId == null) return "알 수 없음";
        return productRepository.findById(productId)
                .map(p -> p.getProductName())
                .orElse("상품(id:" + productId + ")");
    }

    /**
     * order-created 토픽 디버깅용 Consumer
     *
     * - 값(value)은 JsonDeserializer에 의해 Map 형태로 역직렬화됩니다.
     * - 토픽/파티션/오프셋/키와 함께 payload 전체를 로그로 남깁니다.
     * - concurrency: 동시에 몇 개의 Consumer 스레드를 띄울지 지정 (여기서는 1로 명시)
     * - containerFactory: 어떤 Listener Container 설정을 사용할지 지정
     *   (KafkaConfig.genericKafkaListenerContainerFactory 사용 → value를 Object/Map 등으로 처리)
     */
    @KafkaListener(
            topics = "${kafka.topics.order-created:order-created}",
            groupId = "debug-order-created-consumer",
            concurrency = "1",
            containerFactory = "genericKafkaListenerContainerFactory"
    )
    public void listenOrderCreated(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        logger.info("[DEBUG][Kafka] order-created 수신: topic={}, partition={}, offset={}, key={}, payload={}",
                topic, partition, offset, key, payload);
    }

    /**
     * 같은 product-click 토픽을 구독하지만, 다른 groupId와 다른 책임을 가지는 예시 Consumer
     *
     * - groupId: "audit-product-click-consumer"
     *   → debug 용 Consumer 와는 완전히 독립된 컨슈머 그룹으로 동작 (메시지를 각자 따로 소비)
     * - 책임: 추후 감사(Audit)나 별도 모니터링용 로직을 넣을 수 있는 자리
     *   (현재는 예시로 로그 레벨만 다르게 남깁니다.)
     */
    @KafkaListener(
            topics = "${kafka.topics.product-click:product-click}",
            groupId = "audit-product-click-consumer",
            concurrency = "1",
            containerFactory = "genericKafkaListenerContainerFactory"
    )
    public void listenProductClickAudit(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        Long productId = numberFromPayload(payload, "productId");
        Long memberId = numberFromPayload(payload, "memberId");
        String userLabel = resolveUserLabel(memberId);
        String productLabel = resolveProductLabel(productId);
        logger.debug("{} 사용자가 {} 상품을 클릭했습니다. (audit) topic={}, partition={}, offset={}",
                userLabel, productLabel, topic, partition, offset);
    }
}


