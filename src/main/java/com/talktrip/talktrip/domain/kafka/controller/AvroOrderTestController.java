package com.talktrip.talktrip.domain.kafka.controller;

import com.talktrip.talktrip.domain.messaging.dto.order.OrderCreatedEventDTO;
import com.talktrip.talktrip.domain.messaging.dto.order.OrderItemEventDTO;
import com.talktrip.talktrip.domain.messaging.avro.AvroEventProducer;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Avro 이벤트 테스트 Controller
 * 
 * Avro 형식으로 Order와 Product 이벤트를 발행하는 테스트용 엔드포인트를 제공합니다.
 * 실제 비즈니스 로직과는 별도로 Avro 이벤트 발행 기능을 테스트할 수 있습니다.
 */
@Tag(name = "Avro Event Test", description = "Avro 이벤트 테스트 API")
@RestController
@RequestMapping("/api/avro")
@RequiredArgsConstructor
public class AvroOrderTestController {

    private static final Logger logger = LoggerFactory.getLogger(AvroOrderTestController.class);

    private final AvroEventProducer avroEventProducer;

    /**
     * Avro 주문 이벤트 발행 (테스트용)
     * 
     * 간단한 파라미터로 Avro 형식의 주문 이벤트를 발행합니다.
     * 즉, Avro 레코드를 생성하는 시작점(entry point)입니다.
     * 
     * @param customerId 고객 ID (기본값: "CUST-123")
     * @param quantity 수량 (기본값: 5)
     * @param price 가격 (기본값: 99.99)
     * @param productId 상품 ID (기본값: 1L)
     * @param productOptionId 상품 옵션 ID (기본값: 1L)
     * @return 발행 결과
     */
    @Operation(
            summary = "Avro 주문 이벤트 발행 (테스트)",
            description = "Avro 형식으로 주문 이벤트를 발행합니다. 테스트용 엔드포인트입니다."
    )
    @PostMapping("/publish")
    public ResponseEntity<Map<String, Object>> publishAvroOrderEvent(
            @RequestParam(defaultValue = "CUST-123") String customerId,
            @RequestParam(defaultValue = "5") Integer quantity,
            @RequestParam(defaultValue = "99.99") Double price,
            @RequestParam(defaultValue = "1") Long productId,
            @RequestParam(defaultValue = "1") Long productOptionId
    ) {
        try {
            // orderId 생성
            String orderId = UUID.randomUUID().toString();
            Long orderIdLong = Math.abs((long) orderId.hashCode()); // Long 타입으로 변환

            // memberId 변환 (CUST-123 형식에서 숫자 추출, 없으면 해시코드 사용)
            Long memberId;
            try {
                String customerIdNumber = customerId.replace("CUST-", "").trim();
                if (customerIdNumber.isEmpty()) {
                    memberId = Math.abs((long) customerId.hashCode());
                } else {
                    memberId = Long.parseLong(customerIdNumber);
                }
            } catch (NumberFormatException e) {
                memberId = Math.abs((long) customerId.hashCode());
            }

            // OrderItemEventDTO 생성
            OrderItemEventDTO item = OrderItemEventDTO.builder()
                    .productId(productId)
                    .productOptionId(productOptionId)
                    .quantity(quantity)
                    .price(price.intValue())
                    .build();

            // OrderCreatedEventDTO 생성
            OrderCreatedEventDTO eventDTO = OrderCreatedEventDTO.builder()
                    .orderId(orderIdLong)
                    .orderCode("ORDER-" + orderId.substring(0, 8).toUpperCase())
                    .memberId(memberId)
                    .totalPrice(price.intValue() * quantity)
                    .orderStatus("PENDING")
                    .createdAt(LocalDateTime.now())
                    .items(List.of(item))
                    .build();

            // Avro 형식으로 이벤트 발행
            avroEventProducer.publishOrderEvent(eventDTO);
            
            logger.info("Avro 주문 이벤트 발행 완료: orderId={}, orderCode={}, customerId={}, quantity={}, price={}", 
                    orderId, eventDTO.getOrderCode(), customerId, quantity, price);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("orderId", orderId);
            response.put("orderCode", eventDTO.getOrderCode());
            response.put("message", "Avro order event published successfully");
            response.put("eventDTO", eventDTO);

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Avro 주문 이벤트 발행 실패", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Avro order event publish failed: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * Avro 상품 클릭 이벤트 발행 (테스트용)
     * 
     * 간단한 파라미터로 Avro 형식의 상품 클릭 이벤트를 발행합니다.
     * 
     * @param productId 상품 ID (기본값: 1)
     * @return 발행 결과
     */
    @Operation(
            summary = "Avro 상품 클릭 이벤트 발행 (테스트)",
            description = "Avro 형식으로 상품 클릭 이벤트를 발행합니다. 테스트용 엔드포인트입니다."
    )
    @PostMapping("/product-click/publish")
    public ResponseEntity<Map<String, Object>> publishAvroProductClickEvent(
            @RequestParam(defaultValue = "1") Long productId
    ) {
        try {
            // Avro 형식으로 이벤트 발행
            avroEventProducer.publishProductClick(productId);
            
            logger.info("Avro 상품 클릭 이벤트 발행 완료: productId={}", productId);

            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("productId", productId);
            response.put("message", "Avro product click event published successfully");

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Avro 상품 클릭 이벤트 발행 실패", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("success", false);
            errorResponse.put("message", "Avro product click event publish failed: " + e.getMessage());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
}

