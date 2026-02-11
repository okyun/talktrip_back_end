package com.talktrip.talktrip.domain.event.product.listener;

import com.talktrip.talktrip.domain.messaging.dto.product.ProductEvent;
import com.talktrip.talktrip.domain.event.product.ProductClickedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * 상품 클릭 이벤트 리스너
 * 
 * 상품 클릭 이벤트를 수신하여 처리합니다.
 * 
 * 주요 처리 내용:
 * - 상품 클릭 통계 업데이트
 * - 사용자 행동 분석
 * - 추천 시스템 데이터 수집
 */
@Component
public class ProductClickedEventListener {

    private static final Logger logger = LoggerFactory.getLogger(ProductClickedEventListener.class);

    /**
     * 상품 클릭 이벤트 처리
     * 
     * @Async를 사용하여 비동기로 처리합니다.
     * 
     * @param event 상품 클릭 이벤트
     */
    @Async
    @EventListener
    public void handleProductClicked(ProductClickedEvent event) {
        try {
            ProductEvent eventDTO = event.getEventDTO();
            
            logger.info("상품 클릭 이벤트 수신: productId={}, memberId={}, eventTimestamp={}", 
                    eventDTO.getProductId(), 
                    eventDTO.getMemberId(),
                    eventDTO.getEventTimestamp());

            // 실제 비즈니스 로직 처리
            processProductClickedEvent(eventDTO);

            logger.info("상품 클릭 이벤트 처리 완료: productId={}", eventDTO.getProductId());
        } catch (Exception e) {
            logger.error("상품 클릭 이벤트 처리 실패: productId={}", 
                    event.getEventDTO().getProductId(), e);
        }
    }

    /**
     * 상품 클릭 이벤트 처리 로직
     * 
     * 실제 비즈니스 로직을 처리합니다.
     * 예: 클릭 통계 업데이트, 사용자 행동 분석, 추천 시스템 데이터 수집 등
     * 
     * @param eventDTO 상품 클릭 이벤트 DTO
     */
    private void processProductClickedEvent(ProductEvent eventDTO) {
        // TODO: 실제 비즈니스 로직 구현
        // 예: 
        // - 상품 클릭 통계 업데이트 (Kafka Streams에서 처리 가능)
        // - 사용자 행동 분석 데이터 수집
        // - 추천 시스템을 위한 데이터 수집
        // - 실시간 인기 상품 랭킹 업데이트
    }
}

