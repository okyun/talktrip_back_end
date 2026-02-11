package com.talktrip.talktrip.domain.event.order;

import com.talktrip.talktrip.domain.messaging.dto.order.OrderCreatedEventDTO;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

/**
 * 주문 생성 이벤트
 * 
 * Spring의 ApplicationEvent를 상속받아 내부 이벤트로 사용합니다.
 * OrderService에서 주문이 생성되면 이 이벤트를 발행하고,
 * 이벤트 리스너들이 이를 처리합니다.
 */
@Getter
public class OrderCreatedEvent extends ApplicationEvent {
    
    private final OrderCreatedEventDTO eventDTO;
    
    public OrderCreatedEvent(Object source, OrderCreatedEventDTO eventDTO) {
        super(source);
        this.eventDTO = eventDTO;
    }
}

