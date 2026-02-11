package com.talktrip.talktrip.domain.event.order.listener;

import com.talktrip.talktrip.domain.messaging.dto.order.OrderCreatedEventDTO;
import com.talktrip.talktrip.domain.event.order.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * 주문 생성 이벤트 리스너
 * 
 * 주문 생성 이벤트를 수신하여 처리합니다.
 * 기존 AvroOrderEventConsumer의 로직을 이벤트 리스너로 이동했습니다.
 */
@Component
public class OrderCreatedEventListener {

    private static final Logger logger = LoggerFactory.getLogger(OrderCreatedEventListener.class);

    /**
     * 주문 생성 이벤트 처리
     * 
     * @Async를 사용하여 비동기로 처리합니다.
     * 
     * @param event 주문 생성 이벤트
     */
    @Async
    @EventListener
    public void handleOrderCreated(OrderCreatedEvent event) {
        try {
            OrderCreatedEventDTO eventDTO = event.getEventDTO();
            
            logger.info("주문 생성 이벤트 수신: orderId={}, orderCode={}, memberId={}, totalPrice={}, itemsCount={}", 
                    eventDTO.getOrderId(), 
                    eventDTO.getOrderCode(),
                    eventDTO.getMemberId(),
                    eventDTO.getTotalPrice(),
                    eventDTO.getItems() != null ? eventDTO.getItems().size() : 0);

            // 실제 비즈니스 로직 처리
            processOrderCreatedEvent(eventDTO);

            logger.info("주문 생성 이벤트 처리 완료: orderId={}", eventDTO.getOrderId());
        } catch (Exception e) {
            logger.error("주문 생성 이벤트 처리 실패: orderId={}", 
                    event.getEventDTO().getOrderId(), e);
        }
    }

    /**
     * 주문 생성 이벤트 처리 로직
     * 
     * 실제 비즈니스 로직을 처리합니다.
     * 예: 재고 업데이트, 통계 업데이트, 알림 발송 등
     * 
     * @param eventDTO 주문 생성 이벤트 DTO
     */
    private void processOrderCreatedEvent(OrderCreatedEventDTO eventDTO) {
        // TODO: 실제 비즈니스 로직 구현
        // 예: 재고 업데이트, 통계 업데이트, 알림 발송 등
    }
}

