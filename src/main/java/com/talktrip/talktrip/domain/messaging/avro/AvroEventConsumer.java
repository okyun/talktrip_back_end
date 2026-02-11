package com.talktrip.talktrip.domain.messaging.avro;

import com.talktrip.talktrip.domain.messaging.dto.order.OrderCreatedEventDTO;
import com.talktrip.talktrip.domain.messaging.dto.order.OrderItemEventDTO;
import com.talktrip.talktrip.domain.event.order.OrderEventPublisher;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Avro 이벤트 소비자
 * 
 * 외부 시스템에서 발행한 Avro 형식의 주문 이벤트를 수신하여 처리합니다.
 * 
 * 역할:
 * - 외부 시스템에서 오는 Avro 메시지 수신
 * - GenericRecord를 DTO로 변환
 * - 내부 이벤트로 변환하여 처리 (ApplicationEventPublisher 사용)
 * 
 * 주의:
 * - 내부 처리용 Consumer는 ApplicationEventPublisher/EventListener 패턴으로 대체되었습니다.
 * - 이 Consumer는 외부 시스템(다른 서비스, 외부 파이프라인 등)에서 발행한 메시지를 받기 위한 용도입니다.
 */
@Component
@RequiredArgsConstructor
public class AvroEventConsumer {

    private static final Logger logger = LoggerFactory.getLogger(AvroEventConsumer.class);

    @SuppressWarnings("unused")
    private final OrderEventPublisher orderEventPublisher; // TODO: 외부 이벤트를 내부 이벤트로 변환할 때 사용

    // 주문 생성 이벤트를 수신할 토픽
    @Value("${kafka.topics.order-created:order-created}")
    private String orderCreatedTopic;

    /**
     * 주문 생성 이벤트 수신 (Avro 형식)
     * 
     * 외부 시스템에서 발행한 Avro 형식의 주문 생성 이벤트를 수신합니다.
     * GenericRecord를 OrderCreatedEventDTO로 변환한 후,
     * 내부 이벤트로 발행하여 처리합니다.
     * 
     * @param record Avro GenericRecord
     * @param key Kafka 메시지 키
     * @param acknowledgment 수동 커밋을 위한 Acknowledgment (선택적)
     */
    @KafkaListener(
            topics = "${kafka.topics.order-created:order-created}",
            containerFactory = "avroKafkaListenerContainerFactory",
            groupId = "${spring.kafka.consumer.group-id:order-event-consumer-group}"
    )
    public void consumeOrderCreated(
            @Payload GenericRecord record,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {
        
        try {
            logger.info("Avro 주문 생성 이벤트 수신: key={}, topic={}, partition={}, offset={}", 
                    key, topic, partition, offset);

            // GenericRecord를 OrderCreatedEventDTO로 변환
            OrderCreatedEventDTO eventDTO = toOrderCreatedEventDTO(record);

            logger.info("Avro 주문 생성 이벤트 변환 완료: orderId={}, orderCode={}, memberId={}, totalPrice={}, itemsCount={}", 
                    eventDTO.getOrderId(), 
                    eventDTO.getOrderCode(),
                    eventDTO.getMemberId(),
                    eventDTO.getTotalPrice(),
                    eventDTO.getItems() != null ? eventDTO.getItems().size() : 0);

            // TODO: 외부 시스템에서 받은 이벤트를 내부 이벤트로 변환하여 처리
            // 예: 외부 주문 시스템에서 받은 주문을 내부 시스템에 동기화
            // orderEventPublisher.publishOrderCreatedFromExternal(eventDTO);

            // 수동 커밋이 활성화된 경우
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                logger.debug("Avro 주문 생성 이벤트 커밋 완료: orderId={}", eventDTO.getOrderId());
            }

        } catch (Exception e) {
            logger.error("Avro 주문 생성 이벤트 처리 실패: key={}, topic={}, partition={}, offset={}", 
                    key, topic, partition, offset, e);
            // 에러 발생 시에도 커밋하지 않으면 재처리됨
            // 필요에 따라 DLQ(Dead Letter Queue)로 전송하거나 에러 처리 로직 추가
        }
    }

    /**
     * Avro GenericRecord를 OrderCreatedEventDTO로 변환
     * 
     * 스키마 필드 타입:
     * - orderId: long
     * - orderCode: string
     * - memberId: nullable long
     * - totalPrice: int (원 단위)
     * - orderStatus: string
     * - createdAt: timestamp-millis (long, epoch milliseconds)
     * - items: array of OrderItemPayload
     *   - productId: long
     *   - productOptionId: nullable long
     *   - quantity: int
     *   - price: int (원 단위)
     * 
     * @param record Avro GenericRecord
     * @return OrderCreatedEventDTO
     */
    private OrderCreatedEventDTO toOrderCreatedEventDTO(GenericRecord record) {
        // OrderItem 리스트 변환
        @SuppressWarnings("unchecked")
        List<GenericRecord> itemRecords = (List<GenericRecord>) record.get("items");
        List<OrderItemEventDTO> itemDTOs = new ArrayList<>();
        
        if (itemRecords != null) {
            for (GenericRecord itemRecord : itemRecords) {
                OrderItemEventDTO itemDTO = OrderItemEventDTO.builder()
                        .productId((Long) itemRecord.get("productId"))
                        .productOptionId((Long) itemRecord.get("productOptionId"))
                        .quantity((Integer) itemRecord.get("quantity"))
                        .price((Integer) itemRecord.get("price"))
                        .build();
                itemDTOs.add(itemDTO);
            }
        }

        // createdAt: epoch milliseconds (long)를 LocalDateTime으로 변환
        Long createdAtMillis = (Long) record.get("createdAt");
        LocalDateTime createdAt = createdAtMillis != null
                ? LocalDateTime.ofInstant(Instant.ofEpochMilli(createdAtMillis), ZoneId.systemDefault())
                : LocalDateTime.now();

        return OrderCreatedEventDTO.builder()
                .orderId((Long) record.get("orderId"))
                .orderCode((String) record.get("orderCode"))
                .memberId((Long) record.get("memberId"))
                .totalPrice((Integer) record.get("totalPrice"))
                .orderStatus((String) record.get("orderStatus"))
                .createdAt(createdAt)
                .items(itemDTOs)
                .build();
    }
}

