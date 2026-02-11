package com.talktrip.talktrip.domain.event.product.listener;

import com.talktrip.talktrip.domain.messaging.dto.product.ProductEvent;
import com.talktrip.talktrip.domain.event.product.ProductLikedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * 상품 좋아요 이벤트 리스너
 * 
 * 상품 좋아요 이벤트를 수신하여 처리합니다.
 * 
 * 주요 처리 내용:
 * - 상품 좋아요 수 업데이트
 * - 사용자 선호도 분석
 * - 추천 시스템 데이터 수집
 */
@Component
public class ProductLikedEventListener {

    private static final Logger logger = LoggerFactory.getLogger(ProductLikedEventListener.class);

    /**
     * 상품 좋아요 이벤트 처리
     * 
     * @Async를 사용하여 비동기로 처리합니다.
     * 
     * @param event 상품 좋아요 이벤트
     */
    @Async
    @EventListener
    public void handleProductLiked(ProductLikedEvent event) {
        try {
            ProductEvent eventDTO = event.getEventDTO();
            
            logger.info("상품 좋아요 이벤트 수신: productId={}, memberId={}, eventTimestamp={}", 
                    eventDTO.getProductId(), 
                    eventDTO.getMemberId(),
                    eventDTO.getEventTimestamp());

            // 실제 비즈니스 로직 처리
            processProductLikedEvent(eventDTO);

            logger.info("상품 좋아요 이벤트 처리 완료: productId={}", eventDTO.getProductId());
        } catch (Exception e) {
            logger.error("상품 좋아요 이벤트 처리 실패: productId={}", 
                    event.getEventDTO().getProductId(), e);
        }
    }

    /**
     * 상품 좋아요 이벤트 처리 로직
     * 
     * 실제 비즈니스 로직을 처리합니다.
     * 예: 좋아요 수 업데이트, 사용자 선호도 분석, 추천 시스템 데이터 수집 등
     * 
     * @param eventDTO 상품 좋아요 이벤트 DTO
     */
    private void processProductLikedEvent(ProductEvent eventDTO) {
        // TODO: 실제 비즈니스 로직 구현
        // 예: 
        // - 상품 좋아요 수 업데이트 (캐시 또는 DB)
        // - 사용자 선호도 분석 데이터 수집
        // - 추천 시스템을 위한 데이터 수집
        // - 인기 상품 랭킹 업데이트
    }
}

