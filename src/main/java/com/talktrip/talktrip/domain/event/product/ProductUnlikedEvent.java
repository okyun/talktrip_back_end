package com.talktrip.talktrip.domain.event.product;

import com.talktrip.talktrip.domain.messaging.dto.product.ProductEvent;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

/**
 * 상품 좋아요 취소 이벤트
 * 
 * Spring의 ApplicationEvent를 상속받아 내부 이벤트로 사용합니다.
 * LikeService에서 상품의 좋아요가 취소되면 이 이벤트를 발행하고,
 * 이벤트 리스너들이 이를 처리합니다.
 */
@Getter
public class ProductUnlikedEvent extends ApplicationEvent {
    
    private final ProductEvent eventDTO;
    
    public ProductUnlikedEvent(Object source, ProductEvent eventDTO) {
        super(source);
        this.eventDTO = eventDTO;
    }
}

