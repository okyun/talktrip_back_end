package com.talktrip.talktrip.domain.event.order;

import com.talktrip.talktrip.domain.messaging.dto.order.PaymentSuccessEventDTO;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

/**
 * 결제 성공 이벤트
 * 
 * Spring의 ApplicationEvent를 상속받아 내부 이벤트로 사용합니다.
 * OrderService에서 결제가 성공하면 이 이벤트를 발행하고,
 * 이벤트 리스너들이 이를 처리합니다.
 */
@Getter
public class PaymentSuccessEvent extends ApplicationEvent {
    
    private final PaymentSuccessEventDTO eventDTO;
    
    public PaymentSuccessEvent(Object source, PaymentSuccessEventDTO eventDTO) {
        super(source);
        this.eventDTO = eventDTO;
    }
}

