package com.talktrip.talktrip.domain.kafka.dto.product;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 윈도우별 상품 클릭 수 DTO
 * 
 * Kafka Streams에서 윈도우(예: 최근 1분, 최근 5분 등) 안에서의
 * 상품 클릭 개수를 표현하는 모델입니다.
 * 
 * 집계 함수에서 사용되며, 상품 클릭이 하나 들어올 때마다 count를 증가시킵니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WindowedProductClickCount {
    
    /**
     * 현재까지 집계된 상품 클릭 수
     * 기본값: 0L
     */
    @Builder.Default
    private Long count = 0L;
    
    /**
     * 상품 클릭이 하나 들어올 때마다 count를 1 증가시키는 편의 메서드
     * 
     * @return 증가된 WindowedProductClickCount
     */
    public WindowedProductClickCount increment() {
        return WindowedProductClickCount.builder()
                .count(this.count + 1)
                .build();
    }
}

