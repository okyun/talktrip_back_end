package com.talktrip.talktrip.domain.stream.controller;

import com.talktrip.talktrip.domain.messaging.avro.AvroEventProducer;
import com.talktrip.talktrip.domain.messaging.dto.order.CreateOrderRequest;
import com.talktrip.talktrip.domain.messaging.dto.order.OrderEvent;
import com.talktrip.talktrip.domain.messaging.dto.order.OrderPurchaseStatResponse;
import com.talktrip.talktrip.domain.event.order.OrderEventPublisher;
import com.talktrip.talktrip.domain.stream.service.OrderStreamsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 주문 관련 Kafka Streams Controller
 * 
 * 주문 구매 통계 조회 및 주문 이벤트 발행 API를 제공합니다.
 * 
 * 주요 기능:
 * 1. 주문 구매 통계 TOP 30 조회
 * 2. 주문 Streams 상태 확인
 * 3. 주문 생성 및 이벤트 발행
 */
@Tag(name = "Order Streams", description = "주문 관련 Kafka Streams API")
@RestController
@RequestMapping("/api/streams/orders")
@RequiredArgsConstructor
public class OrderStreamController {

    private final OrderStreamsService orderStreamsService;
    private final OrderEventPublisher orderEventPublisher;
    private final AvroEventProducer avroEventProducer;

    /**
     * 주문 구매 통계 TOP 30 조회
     * 
     * Kafka Streams State Store에서 주문 구매 통계 TOP 30을 조회합니다.
     * 
     * @param windowStartTime 윈도우 시작 시간 (epoch milliseconds, 선택적)
     *                        - null이면 모든 윈도우의 데이터를 조회하여 전체 TOP 30 반환
     *                        - 값이 있으면 해당 윈도우의 TOP 30 반환
     * @return 주문 구매 통계 TOP 30 리스트
     */
    @Operation(
            summary = "주문 구매 통계 TOP 30 조회",
            description = "Kafka Streams State Store에서 주문 구매 통계 TOP 30을 조회합니다. " +
                         "windowStartTime 파라미터로 특정 윈도우의 통계를 조회하거나, " +
                         "파라미터를 생략하면 모든 윈도우의 데이터를 조회하여 전체 TOP 30을 반환합니다."
    )
    @GetMapping("/purchases/top30")
    public ResponseEntity<List<OrderPurchaseStatResponse>> getTop30OrderPurchases(
            @RequestParam(required = false) Long windowStartTime
    ) {
        List<OrderPurchaseStatResponse> result = orderStreamsService.getTop30OrderPurchases(windowStartTime);
        return ResponseEntity.ok(result);
    }

    /**
     * 주문 Streams 상태 확인
     * 
     * @return 준비되었으면 true, 아니면 false
     */
    @Operation(
            summary = "주문 Streams 상태 확인",
            description = "주문 관련 Kafka Streams가 준비되었는지 확인합니다."
    )
    @GetMapping("/purchases/status")
    public ResponseEntity<Boolean> getOrderStreamsStatus() {
        boolean isReady = orderStreamsService.isReady();
        return ResponseEntity.ok(isReady);
    }

    /**
     * 주문 생성 (OrderEvent 발행)
     * 
     * 서버가 자동으로 orderId를 생성하고,
     * customerId, quantity, price를 파라미터로 받아서 OrderEvent를 생성하고 발행합니다.
     * 
     * @param request CreateOrderRequest (customerId, quantity, price)
     * @return 주문 생성 성공 메시지
     */
    @Operation(
            summary = "주문 생성 (OrderEvent 발행)",
            description = "서버가 자동으로 orderId를 생성하고, customerId, quantity, price를 받아서 OrderEvent를 발행합니다."
    )
    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();
        
        OrderEvent orderEvent = OrderEvent.builder()
                .orderId(orderId)
                .customerId(request.getCustomerId())
                .quantity(request.getQuantity())
                .price(request.getPrice())
                .eventType("ORDER_CREATED")
                .status("PENDING")
                .build();
        
        orderEventPublisher.publishOrderEvent(orderEvent);
        
        return ResponseEntity.ok("Order created");
    }

    /**
     * Avro 주문 이벤트 발행
     * 
     * 서버가 자동으로 orderId를 생성하고,
     * customerId, quantity, price를 파라미터로 받아서 Avro GenericRecord를 생성하고 발행합니다.
     * Avro 스키마(order-entity.avsc) 기반으로 레코드를 생성합니다.
     * 
     * 즉, Avro 레코드를 생성하는 시작점(entry point)입니다.
     * 
     * @param customerId 고객 ID (기본값: "CUST-123")
     * @param quantity 주문 수량 (기본값: 5)
     * @param price 주문 가격 (기본값: 99.99)
     * @return 발행 결과 (success, orderId, message)
     */
    @Operation(
            summary = "Avro 주문 이벤트 발행",
            description = "서버가 자동으로 orderId를 생성하고, Avro 스키마 기반으로 GenericRecord를 생성하여 Kafka에 발행합니다."
    )
    @PostMapping("/avro/publish")
    public ResponseEntity<Map<String, Object>> createOrderAvro(
            @RequestParam(defaultValue = "CUST-123") String customerId,
            @RequestParam(defaultValue = "5") Integer quantity,
            @RequestParam(defaultValue = "99.99") BigDecimal price
    ) {
        String orderId = UUID.randomUUID().toString();
        
        avroEventProducer.publishOrderEvent(orderId, customerId, quantity, price);
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("orderId", orderId);
        response.put("message", "Avro order event published successfully");
        
        return ResponseEntity.ok(response);
    }
}


