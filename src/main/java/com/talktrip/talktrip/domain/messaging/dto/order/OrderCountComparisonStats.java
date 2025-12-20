package com.talktrip.talktrip.domain.messaging.dto.order;

import com.talktrip.talktrip.domain.messaging.dto.PeriodStats;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 주문 수 비교 통계 DTO
 * 
 * 현재 기간과 이전 기간의 주문 수를 비교한 통계 정보입니다.
 * Kafka Streams로 분석/집계할 때 사용하는 응답 데이터입니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCountComparisonStats {
    
    /**
     * 현재 기간의 통계
     */
    private PeriodStats currentPeriod;
    
    /**
     * 이전 기간의 통계
     */
    private PeriodStats previousPeriod;
    
    /**
     * 주문 수 변화량 (현재 - 이전)
     */
    private Long changeCount;
    
    /**
     * 변화율 (%)
     */
    private Double changePercentage;
    
    /**
     * 증가 중인지 여부
     * true: 증가, false: 감소/동일
     */
    private Boolean isIncreasing;
}

