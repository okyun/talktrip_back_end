package com.talktrip.talktrip.domain.kafka.dto.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 주문 생성 요청 DTO
 * 
 * 주문 생성을 위한 요청 데이터 구조입니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderRequest {
    
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
}

