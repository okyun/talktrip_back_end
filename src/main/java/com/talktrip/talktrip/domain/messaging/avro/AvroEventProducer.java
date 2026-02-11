package com.talktrip.talktrip.domain.messaging.avro;

import com.talktrip.talktrip.domain.messaging.dto.order.OrderCreatedEventDTO;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Avro 이벤트 발행자
 * 
 * Order와 Product 이벤트를 Avro 형식으로 Kafka에 발행합니다.
 * SchemaManager를 사용하여 스키마를 관리하고,
 * 엔티티/DTO를 Avro GenericRecord로 변환하여 발행합니다.
 */
@Component
@RequiredArgsConstructor
public class AvroEventProducer {

    private static final Logger logger = LoggerFactory.getLogger(AvroEventProducer.class);

    private final KafkaTemplate<String, GenericRecord> avroKafkaTemplate;
    private final SchemaManager schemaManager;

    // 주문 생성 이벤트를 발행할 토픽
    @Value("${kafka.topics.order-created:order-created}")
    private String orderCreatedTopic;

    // 상품 클릭 이벤트를 발행할 토픽
            @Value("${kafka.topics.product-click:product-click}")
    private String productClickTopic;

    // ========== Order Event Methods ==========

    /**
     * 주문 생성 이벤트 발행 (Avro 형식)
     * 
     * OrderCreatedEventDTO를 Avro GenericRecord로 변환하여 Kafka로 발행합니다.
     * DTO 중심 설계로 엔티티와의 결합도를 낮추고, 이벤트 발행 로직을 명확하게 분리합니다.
     * 
     * @param dto OrderCreatedEventDTO
     */
    public void publishOrderCreated(OrderCreatedEventDTO dto) {
        try {
            GenericRecord avroRecord = toAvroOrderRecord(dto);
            
            // orderId를 키로 사용하여 Avro 형식으로 발행
            avroKafkaTemplate.send(orderCreatedTopic, String.valueOf(dto.getOrderId()), avroRecord)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            logger.info("Avro 주문 생성 이벤트 발행 성공: orderId={}, orderCode={}, topic={}, partition={}, offset={}", 
                                    dto.getOrderId(), dto.getOrderCode(), 
                                    result.getRecordMetadata().topic(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            logger.error("Avro 주문 생성 이벤트 발행 실패: orderId={}, orderCode={}", 
                                    dto.getOrderId(), dto.getOrderCode(), ex);
                        }
                    });
        } catch (Exception e) {
            logger.error("Avro 주문 생성 이벤트 변환 또는 발행 중 오류 발생: orderId={}, orderCode={}", 
                    dto.getOrderId(), dto.getOrderCode(), e);
            throw new RuntimeException("Avro 주문 생성 이벤트 발행 실패", e);
        }
    }

    /**
     * 주문 생성 이벤트 발행 (Avro 형식) - 별칭 메서드
     * 
     * publishOrderCreated와 동일한 기능을 제공하는 별칭 메서드입니다.
     * 기존 코드와의 호환성을 위해 유지됩니다.
     * 
     * @param dto OrderCreatedEventDTO
     */
    public void publishOrderEvent(OrderCreatedEventDTO dto) {
        publishOrderCreated(dto);
    }

    // ========== Product Event Methods ==========

    /**
     * 상품 클릭 이벤트 발행 (Avro 형식)
     * 
     * 상품 클릭 이벤트를 Avro GenericRecord로 변환하여 Kafka로 발행합니다.
     * 키는 productId를 사용합니다.
     * 
     * @param productId 상품 ID
     */
    public void publishProductClick(Long productId) {
        try {
            GenericRecord avroRecord = toAvroProductClickRecord(productId);
            
            // productId를 키로 사용하여 Avro 형식으로 발행
            avroKafkaTemplate.send(productClickTopic, String.valueOf(productId), avroRecord)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            logger.info("Avro 상품 클릭 이벤트 발행 성공: productId={}, topic={}, partition={}, offset={}", 
                                    productId,
                                    result.getRecordMetadata().topic(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            logger.error("Avro 상품 클릭 이벤트 발행 실패: productId={}", productId, ex);
                        }
                    });
        } catch (Exception e) {
            logger.error("Avro 상품 클릭 이벤트 변환 또는 발행 중 오류 발생: productId={}", productId, e);
            throw new RuntimeException("Avro 상품 클릭 이벤트 발행 실패", e);
        }
    }

    /**
     * 상품 클릭 이벤트 발행 (Avro 형식) - String productId
     * 
     * @param productId 상품 ID (String)
     */
    public void publishProductClick(String productId) {
        try {
            Long productIdLong = Long.parseLong(productId);
            publishProductClick(productIdLong);
        } catch (NumberFormatException e) {
            logger.error("상품 ID 파싱 실패: productId={}", productId, e);
            throw new IllegalArgumentException("유효하지 않은 상품 ID: " + productId, e);
        }
    }

    /**
     * 상품 클릭 이벤트 발행 (Avro 형식) - memberId 포함
     * 
     * @param productId 상품 ID
     * @param memberId 회원 ID (null 가능)
     */
    public void publishProductClick(Long productId, Long memberId) {
        try {
            GenericRecord avroRecord = toAvroProductClickRecord(productId, memberId);
            
            // productId를 키로 사용하여 Avro 형식으로 발행
            avroKafkaTemplate.send(productClickTopic, String.valueOf(productId), avroRecord)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            logger.info("Avro 상품 클릭 이벤트 발행 성공: productId={}, memberId={}, topic={}, partition={}, offset={}", 
                                    productId, memberId,
                                    result.getRecordMetadata().topic(),
                                    result.getRecordMetadata().partition(),
                                    result.getRecordMetadata().offset());
                        } else {
                            logger.error("Avro 상품 클릭 이벤트 발행 실패: productId={}, memberId={}", 
                                    productId, memberId, ex);
                        }
                    });
        } catch (Exception e) {
            logger.error("Avro 상품 클릭 이벤트 변환 또는 발행 중 오류 발생: productId={}, memberId={}", 
                    productId, memberId, e);
            throw new RuntimeException("Avro 상품 클릭 이벤트 발행 실패", e);
        }
    }

    /**
     * 상품 클릭 이벤트를 Avro GenericRecord로 변환 (memberId 포함)
     * 
     * @param productId 상품 ID
     * @param memberId 회원 ID (null 가능)
     * @return Avro GenericRecord
     */
    private GenericRecord toAvroProductClickRecord(Long productId, Long memberId) {
        Schema schema = schemaManager.getProductClickEventSchema();
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
        
        recordBuilder.set("productId", productId);
        recordBuilder.set("memberId", memberId); // null 가능
        // timestamp-millis는 long 타입의 epoch milliseconds
        recordBuilder.set("clickedAt", Instant.now().toEpochMilli());
        
        return recordBuilder.build();
    }

    // ========== Avro Record Conversion Methods ==========

    /**
     * OrderCreatedEventDTO를 Avro GenericRecord로 변환
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
     * @param dto OrderCreatedEventDTO
     * @return Avro GenericRecord
     */
    private GenericRecord toAvroOrderRecord(OrderCreatedEventDTO dto) {
        Schema schema = schemaManager.getOrderCreatedEventSchema();
        GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);

        // OrderItem 리스트를 Avro 레코드 리스트로 변환
        Schema itemSchema = schema.getField("items").schema().getElementType();
        List<GenericRecord> itemRecords = dto.getItems().stream()
                .map(item -> {
                    GenericRecordBuilder itemBuilder = new GenericRecordBuilder(itemSchema);
                    itemBuilder.set("productId", item.getProductId());
                    itemBuilder.set("productOptionId", item.getProductOptionId());
                    itemBuilder.set("quantity", item.getQuantity());
                    itemBuilder.set("price", item.getPrice());
                    return itemBuilder.build();
                })
                .collect(Collectors.toList());

        recordBuilder.set("orderId", dto.getOrderId());
        recordBuilder.set("orderCode", dto.getOrderCode());
        recordBuilder.set("memberId", dto.getMemberId());
        recordBuilder.set("totalPrice", dto.getTotalPrice());
        recordBuilder.set("orderStatus", dto.getOrderStatus());
        
        // createdAt: LocalDateTime을 epoch milliseconds (long)로 변환
        // timestamp-millis logicalType은 long 타입의 epoch milliseconds를 요구
        long createdAtMillis = dto.getCreatedAt() != null 
                ? dto.getCreatedAt().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
                : Instant.now().toEpochMilli();
        recordBuilder.set("createdAt", createdAtMillis);
        
        recordBuilder.set("items", itemRecords);

        return recordBuilder.build();
    }

    /**
     * 상품 클릭 이벤트를 Avro GenericRecord로 변환
     * 
     * 스키마 필드:
     * - productId: long (필수)
     * - memberId: nullable long (선택적, 기본값: null)
     * - clickedAt: timestamp-millis (long, epoch milliseconds)
     * 
     * @param productId 상품 ID
     * @return Avro GenericRecord
     */
    private GenericRecord toAvroProductClickRecord(Long productId) {
        return toAvroProductClickRecord(productId, null);
    }

    /**
     * 주문 이벤트 발행 (Avro 형식) - 직접 파라미터로 받기
     * 
     * orderId, customerId, quantity, price를 직접 받아서 Avro 이벤트를 발행합니다.
     * 컨트롤러에서 직접 호출할 때 사용합니다.
     * 
     * @param orderId 주문 ID (UUID 문자열)
     * @param customerId 고객 ID
     * @param quantity 주문 수량
     * @param price 주문 가격 (BigDecimal)
     */
    public void publishOrderEvent(String orderId, String customerId, Integer quantity, java.math.BigDecimal price) {
        try {
            // OrderCreatedEventDTO를 생성하기 위해 최소한의 정보로 구성
            // 실제로는 더 많은 정보가 필요하지만, 테스트/간단한 발행용으로 사용
            // orderId는 UUID 문자열이므로, orderCode로 사용하고 orderId는 임시로 0L 사용
            // (실제 프로덕션에서는 orderId를 Long으로 관리하거나 별도 처리 필요)
            Long memberIdLong = null;
            if (customerId != null && !customerId.isEmpty()) {
                try {
                    // customerId가 숫자 문자열인 경우 Long으로 변환
                    memberIdLong = Long.parseLong(customerId);
                } catch (NumberFormatException e) {
                    // customerId가 숫자가 아닌 경우 (예: "CUST-123") null로 설정
                    logger.warn("customerId를 Long으로 변환할 수 없습니다. null로 설정: customerId={}", customerId);
                }
            }
            
            OrderCreatedEventDTO dto = OrderCreatedEventDTO.builder()
                    .orderId(0L) // UUID 문자열이므로 임시로 0L 사용 (실제로는 orderCode 사용)
                    .orderCode(orderId) // UUID 문자열을 orderCode로 사용
                    .memberId(memberIdLong)
                    .totalPrice(price != null ? price.intValue() : 0)
                    .orderStatus("PENDING")
                    .createdAt(LocalDateTime.now())
                    .items(List.of()) // 빈 리스트 (필요시 확장 가능)
                    .build();
            
            publishOrderCreated(dto);
            logger.info("Avro 주문 이벤트 발행 성공 (직접 파라미터): orderId={}, customerId={}, quantity={}, price={}", 
                    orderId, customerId, quantity, price);
        } catch (Exception e) {
            logger.error("Avro 주문 이벤트 발행 실패 (직접 파라미터): orderId={}, customerId={}", 
                    orderId, customerId, e);
            throw new RuntimeException("Avro 주문 이벤트 발행 실패", e);
        }
    }

    /**
     * 상품 이벤트 발행 (Avro 형식) - 직접 파라미터로 받기
     * 
     * productId, memberId를 직접 받아서 Avro 상품 클릭 이벤트를 발행합니다.
     * 
     * @param productId 상품 ID
     * @param memberId 회원 ID (nullable)
     */
    public void publishProductEvent(Long productId, Long memberId) {
        publishProductClick(productId, memberId);
        logger.info("Avro 상품 이벤트 발행 성공 (직접 파라미터): productId={}, memberId={}", productId, memberId);
    }
}

