package com.talktrip.talktrip.domain.messaging.dto.product;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * 상품 조회 통계 응답 DTO
 * 
 * 특정 기간 동안의 상품 조회 통계를 표현합니다.
 * ProductClickStatResponse와 유사하지만 조회(VIEW) 중심의 통계입니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductViewStatResponse {
    
    /**
     * 상품 ID
     */
    private String productId;
    
    /**
     * 조회 수
     */
    private Long viewCount;
    
    /**
     * 클릭 수
     */
    private Long clickCount;
    
    /**
     * 윈도우 시작 시간
     */
    private Instant windowStart;
    
    /**
     * 윈도우 종료 시간
     */
    private Instant windowEnd;
    
    /**
     * 클릭률 (클릭 수 / 조회 수 * 100)
     * 조회 수가 0이면 0.0 반환
     */
    public Double getClickThroughRate() {
        if (viewCount == null || viewCount == 0) {
            return 0.0;
        }
        if (clickCount == null) {
            return 0.0;
        }
        return (clickCount.doubleValue() / viewCount.doubleValue()) * 100.0;
    }
}

