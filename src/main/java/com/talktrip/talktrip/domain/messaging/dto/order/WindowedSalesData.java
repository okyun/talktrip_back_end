package com.talktrip.talktrip.domain.messaging.dto.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 윈도우별 매출 통계 DTO
 * 
 * Kafka Streams에서 윈도우 안에서의 매출 통계를 표현하는 모델입니다.
 * 총 매출 금액과 주문 수를 함께 관리합니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WindowedSalesData {
    
    /**
     * 해당 기간 총 매출 금액
     * 기본값: BigDecimal.ZERO
     */
    @Builder.Default
    private BigDecimal totalSales = BigDecimal.ZERO;
    
    /**
     * 해당 기간 주문 수
     * 기본값: 0L
     */
    @Builder.Default
    private Long orderCount = 0L;
    
    /**
     * 새로운 주문 금액을 더하면서, 주문 수를 1 증가시키는 메서드
     * 
     * @param orderValue 주문 금액
     * @return 업데이트된 WindowedSalesData
     */
    public WindowedSalesData add(BigDecimal orderValue) {
        return WindowedSalesData.builder()
                .totalSales(this.totalSales.add(orderValue))
                .orderCount(this.orderCount + 1)
                .build();
    }
}

