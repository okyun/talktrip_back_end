package com.talktrip.talktrip.domain.event.product;

import com.talktrip.talktrip.domain.messaging.dto.product.ProductEvent;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

/**
 * 상품 클릭 이벤트
 * 
 * Spring의 ApplicationEvent를 상속받아 내부 이벤트로 사용합니다.
 * ProductService나 Controller에서 상품이 클릭되면 이 이벤트를 발행하고,
 * 이벤트 리스너들이 이를 처리합니다.
 */
@Getter
public class ProductClickedEvent extends ApplicationEvent {
    
    private final ProductEvent eventDTO;
    
    public ProductClickedEvent(Object source, ProductEvent eventDTO) {
        super(source);
        this.eventDTO = eventDTO;
    }
}

