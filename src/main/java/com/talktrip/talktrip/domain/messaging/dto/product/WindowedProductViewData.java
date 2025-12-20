package com.talktrip.talktrip.domain.messaging.dto.product;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 윈도우별 상품 조회 통계 DTO
 * 
 * Kafka Streams에서 윈도우 안에서의 상품 조회 통계를 표현하는 모델입니다.
 * 총 조회 수와 클릭 수를 함께 관리합니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WindowedProductViewData {
    
    /**
     * 해당 기간 총 조회 수
     * 기본값: 0L
     */
    @Builder.Default
    private Long totalViews = 0L;
    
    /**
     * 해당 기간 클릭 수
     * 기본값: 0L
     */
    @Builder.Default
    private Long clickCount = 0L;
    
    /**
     * 상품 조회가 하나 들어올 때마다 조회 수를 1 증가시키는 메서드
     * 
     * @return 업데이트된 WindowedProductViewData
     */
    public WindowedProductViewData incrementView() {
        return WindowedProductViewData.builder()
                .totalViews(this.totalViews + 1)
                .clickCount(this.clickCount)
                .build();
    }
    
    /**
     * 상품 클릭이 하나 들어올 때마다 클릭 수를 1 증가시키는 메서드
     * 
     * @return 업데이트된 WindowedProductViewData
     */
    public WindowedProductViewData incrementClick() {
        return WindowedProductViewData.builder()
                .totalViews(this.totalViews)
                .clickCount(this.clickCount + 1)
                .build();
    }
    
    /**
     * 조회와 클릭을 동시에 증가시키는 메서드
     * 
     * @return 업데이트된 WindowedProductViewData
     */
    public WindowedProductViewData incrementViewAndClick() {
        return WindowedProductViewData.builder()
                .totalViews(this.totalViews + 1)
                .clickCount(this.clickCount + 1)
                .build();
    }
}

