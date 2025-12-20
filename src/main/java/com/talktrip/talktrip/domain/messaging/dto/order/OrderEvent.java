package com.talktrip.talktrip.domain.messaging.dto.order;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 주문 이벤트 DTO
 * 
 * Kafka로 발행되는 주문 이벤트의 기본 구조입니다.
 * 주문 생성, 취소, 업데이트 등 모든 주문 관련 이벤트에 사용됩니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {

    /**
     * 주문 ID
     */
    private String orderId;
    
    /**
     * 고객 ID (회원 ID)
     */
    private String customerId;
    
    /**
     * 주문 수량
     */
    private Integer quantity;
    
    /**
     * 주문 가격
     */
    private BigDecimal price;
    
    /**
     * 이벤트 타입
     * 기본값: "ORDER_CREATED"
     * ORDER_CREATED, ORDER_CANCELLED, ORDER_UPDATED, ORDER_COMPLETED 등
     */
    @Builder.Default
    private String eventType = "ORDER_CREATED";
    
    /**
     * 주문 상태
     * 기본값: "PENDING"
     * PENDING, SUCCESS, CANCELLED 등
     */
    @Builder.Default
    private String status = "PENDING";
    
    /**
     * 이벤트 발생 시각
     * 기본값: 현재 시각
     * JSON 직렬화 시 "yyyy-MM-dd'T'HH:mm:ss" 형식으로 변환
     */
    @Builder.Default
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp = LocalDateTime.now();
}

