package com.talktrip.talktrip.domain.kafka.dto.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 윈도우별 주문 수 DTO
 * 
 * Kafka Streams에서 윈도우(예: 최근 1분, 최근 5분 등) 안에서의
 * 주문 개수를 표현하는 모델입니다.
 * 
 * 집계 함수에서 사용되며, 주문이 하나 들어올 때마다 count를 증가시킵니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WindowedOrderCount {

    /**
     * 현재까지 집계된 주문 수
     * 기본값: 0L
     */
    @Builder.Default
    private Long count = 0L;
    
    /**
     * 주문이 하나 들어올 때마다 count를 1 증가시키는 편의 메서드
     * 
     * @return 증가된 WindowedOrderCount
     */
    public WindowedOrderCount increment() {
        return WindowedOrderCount.builder()
                .count(this.count + 1)
                .build();
    }
}

