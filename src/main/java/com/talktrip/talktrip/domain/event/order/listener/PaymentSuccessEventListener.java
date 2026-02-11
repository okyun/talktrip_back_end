package com.talktrip.talktrip.domain.event.order.listener;

import com.talktrip.talktrip.domain.messaging.dto.order.PaymentSuccessEventDTO;
import com.talktrip.talktrip.domain.event.order.PaymentSuccessEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * 결제 성공 이벤트 리스너
 * 
 * 결제 성공 이벤트를 수신하여 처리합니다.
 * 기존 PaymentSuccessConsumer의 로직을 이벤트 리스너로 이동했습니다.
 */
@Component
public class PaymentSuccessEventListener {

    private static final Logger logger = LoggerFactory.getLogger(PaymentSuccessEventListener.class);

    /**
     * 결제 성공 이벤트 처리
     * 
     * @Async를 사용하여 비동기로 처리합니다.
     * 
     * @param event 결제 성공 이벤트
     */
    @Async
    @EventListener
    public void handlePaymentSuccess(PaymentSuccessEvent event) {
        try {
            PaymentSuccessEventDTO eventDTO = event.getEventDTO();
            
            logger.info("결제 성공 이벤트 수신: orderId={}, orderCode={}, paymentKey={}, totalAmount={}", 
                    eventDTO.getOrderId(), eventDTO.getOrderCode(), eventDTO.getPaymentKey(), eventDTO.getTotalAmount());

            // 1. 결제 완료 알림 발송
            sendPaymentSuccessNotification(eventDTO);

            // 2. 결제 통계 업데이트
            updatePaymentStatistics(eventDTO);

            // 3. 주문 상태 확인 및 업데이트 (필요 시)
            verifyOrderStatus(eventDTO);

            logger.info("결제 성공 이벤트 처리 완료: orderId={}, orderCode={}", 
                    eventDTO.getOrderId(), eventDTO.getOrderCode());
        } catch (Exception e) {
            logger.error("결제 성공 이벤트 처리 중 오류 발생: orderId={}", 
                    event.getEventDTO().getOrderId(), e);
        }
    }

    /**
     * 결제 완료 알림 발송
     * 
     * 결제가 완료되면 사용자에게 알림을 발송합니다.
     */
    private void sendPaymentSuccessNotification(PaymentSuccessEventDTO eventDTO) {
        try {
            logger.info("결제 완료 알림 발송: orderId={}, orderCode={}, totalAmount={}", 
                    eventDTO.getOrderId(), eventDTO.getOrderCode(), eventDTO.getTotalAmount());
            
            // TODO: 실제 알림 발송 로직 구현
            // - 이메일 발송
            // - SMS 발송
            // - 푸시 알림 발송
            // - WebSocket을 통한 실시간 알림
            
        } catch (Exception e) {
            logger.error("결제 완료 알림 발송 실패: orderId={}", eventDTO.getOrderId(), e);
            // 알림 발송 실패는 결제 처리에 영향을 주지 않음
        }
    }

    /**
     * 결제 통계 업데이트
     * 
     * 결제 완료 시 통계 정보를 업데이트합니다.
     */
    private void updatePaymentStatistics(PaymentSuccessEventDTO eventDTO) {
        try {
            logger.info("결제 통계 업데이트: orderId={}, method={}, totalAmount={}", 
                    eventDTO.getOrderId(), eventDTO.getMethod(), eventDTO.getTotalAmount());
            
            // TODO: 실제 통계 업데이트 로직 구현
            // - 일별/월별 결제 통계
            // - 결제 수단별 통계
            // - 결제 제공자별 통계
            
        } catch (Exception e) {
            logger.error("결제 통계 업데이트 실패: orderId={}", eventDTO.getOrderId(), e);
            // 통계 업데이트 실패는 결제 처리에 영향을 주지 않음
        }
    }

    /**
     * 주문 상태 확인 및 업데이트
     * 
     * 결제 완료 후 주문 상태가 올바르게 업데이트되었는지 확인합니다.
     */
    private void verifyOrderStatus(PaymentSuccessEventDTO eventDTO) {
        try {
            logger.debug("주문 상태 확인: orderId={}, orderCode={}", 
                    eventDTO.getOrderId(), eventDTO.getOrderCode());
            
            // TODO: 실제 주문 상태 확인 로직 구현
            // - 주문 상태가 SUCCESS인지 확인
            // - 결제 정보가 올바르게 연결되었는지 확인
            // - 필요 시 상태 업데이트
            
        } catch (Exception e) {
            logger.error("주문 상태 확인 실패: orderId={}", eventDTO.getOrderId(), e);
            // 주문 상태 확인 실패는 결제 처리에 영향을 주지 않음
        }
    }
}

