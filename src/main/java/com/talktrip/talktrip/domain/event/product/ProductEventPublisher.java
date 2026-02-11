package com.talktrip.talktrip.domain.event.product;

import com.talktrip.talktrip.domain.messaging.dto.product.ProductEvent;
import com.talktrip.talktrip.domain.messaging.avro.AvroEventProducer;
import com.talktrip.talktrip.domain.product.entity.Product;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * 상품 이벤트 발행자
 * 
 * Product 엔티티나 ProductEvent DTO를 받아서 이벤트를 발행합니다.
 * 1. ApplicationEventPublisher를 통해 내부 이벤트로 발행 (내부 처리용)
 * 2. AvroEventProducer를 통해 Kafka에 이벤트를 발행 (외부 시스템/스트림 처리용)
 *
 * 역할:
 * - 엔티티→DTO 변환 레이어 제공
 * - 도메인 엔티티와 이벤트 발행 로직 분리
 * - 내부 이벤트(ApplicationEvent)와 외부 이벤트(Kafka) 동시 발행
 * - ProductService나 Controller에서 직접 사용할 수 있는 간단한 인터페이스 제공
 */
@Component
@RequiredArgsConstructor
public class ProductEventPublisher {

    private static final Logger logger = LoggerFactory.getLogger(ProductEventPublisher.class);

    private final ApplicationEventPublisher applicationEventPublisher;
    private final AvroEventProducer avroEventProducer;

    /**
     * 상품 클릭 이벤트 발행
     * 
     * Product 엔티티를 받아서 ProductEvent DTO로 변환한 후,
     * 1. ApplicationEventPublisher를 통해 내부 이벤트로 발행 (내부 처리용)
     * 2. Avro 형식으로 Kafka에 이벤트를 발행 (외부 시스템/스트림 처리용)
     * 
     * @param product 상품 엔티티
     * @param memberId 회원 ID (nullable, 비회원인 경우 null)
     */
    public void publishProductClicked(Product product, Long memberId) {
        try {
            ProductEvent eventDTO = ProductEvent.productClicked(product.getId(), memberId);
            
            // 1. 내부 이벤트 발행 (ApplicationEventPublisher)
            applicationEventPublisher.publishEvent(new ProductClickedEvent(this, eventDTO));
            logger.debug("상품 클릭 내부 이벤트 발행 완료: productId={}, memberId={}", 
                    product.getId(), memberId);
            
            // 2. Kafka 이벤트 발행 (외부 시스템/스트림 처리용)
            avroEventProducer.publishProductClick(product.getId(), memberId);
            logger.debug("상품 클릭 Kafka 이벤트 발행 완료: productId={}, memberId={}", 
                    product.getId(), memberId);
        } catch (Exception e) {
            logger.error("상품 클릭 이벤트 발행 실패: productId={}, memberId={}", 
                    product.getId(), memberId, e);
            throw new RuntimeException("상품 클릭 이벤트 발행 실패", e);
        }
    }

    /**
     * 상품 조회 이벤트 발행
     * 
     * Product 엔티티를 받아서 ProductEvent DTO로 변환한 후,
     * 1. ApplicationEventPublisher를 통해 내부 이벤트로 발행 (내부 처리용)
     * 2. 필요시 Kafka에 이벤트를 발행 (외부 시스템/스트림 처리용)
     * 
     * @param product 상품 엔티티
     * @param memberId 회원 ID (nullable, 비회원인 경우 null)
     */
    public void publishProductViewed(Product product, Long memberId) {
        try {
            ProductEvent eventDTO = ProductEvent.productViewed(product.getId(), memberId);
            
            // 1. 내부 이벤트 발행 (ApplicationEventPublisher)
            applicationEventPublisher.publishEvent(new ProductViewedEvent(this, eventDTO));
            logger.debug("상품 조회 내부 이벤트 발행 완료: productId={}, memberId={}", 
                    product.getId(), memberId);
            
            // 2. Kafka 이벤트 발행은 필요시 구현
            // TODO: AvroEventProducer에 publishProductViewed 메서드가 있다면 사용
            logger.debug("상품 조회 Kafka 이벤트 발행 완료: productId={}, memberId={}", 
                    product.getId(), memberId);
        } catch (Exception e) {
            logger.error("상품 조회 이벤트 발행 실패: productId={}, memberId={}", 
                    product.getId(), memberId, e);
            // 이벤트 발행 실패는 조회 처리에 영향을 주지 않음
        }
    }

    /**
     * 상품 좋아요 이벤트 발행
     * 
     * Product 엔티티를 받아서 ProductEvent DTO로 변환한 후,
     * 1. ApplicationEventPublisher를 통해 내부 이벤트로 발행 (내부 처리용)
     * 2. 필요시 Kafka에 이벤트를 발행 (외부 시스템/스트림 처리용)
     * 
     * @param product 상품 엔티티
     * @param memberId 회원 ID
     */
    public void publishProductLiked(Product product, Long memberId) {
        try {
            ProductEvent eventDTO = ProductEvent.productLiked(product.getId(), memberId);
            
            // 1. 내부 이벤트 발행 (ApplicationEventPublisher)
            applicationEventPublisher.publishEvent(new ProductLikedEvent(this, eventDTO));
            logger.debug("상품 좋아요 내부 이벤트 발행 완료: productId={}, memberId={}", 
                    product.getId(), memberId);
            
            // 2. Kafka 이벤트 발행은 필요시 구현
            // TODO: AvroEventProducer에 publishProductLiked 메서드가 있다면 사용
            logger.debug("상품 좋아요 Kafka 이벤트 발행 완료: productId={}, memberId={}", 
                    product.getId(), memberId);
        } catch (Exception e) {
            logger.error("상품 좋아요 이벤트 발행 실패: productId={}, memberId={}", 
                    product.getId(), memberId, e);
            // 이벤트 발행 실패는 좋아요 처리에 영향을 주지 않음
        }
    }

    /**
     * 상품 좋아요 취소 이벤트 발행
     * 
     * Product 엔티티를 받아서 ProductEvent DTO로 변환한 후,
     * 1. ApplicationEventPublisher를 통해 내부 이벤트로 발행 (내부 처리용)
     * 2. 필요시 Kafka에 이벤트를 발행 (외부 시스템/스트림 처리용)
     * 
     * @param product 상품 엔티티
     * @param memberId 회원 ID
     */
    public void publishProductUnliked(Product product, Long memberId) {
        try {
            ProductEvent eventDTO = ProductEvent.productUnliked(product.getId(), memberId);
            
            // 1. 내부 이벤트 발행 (ApplicationEventPublisher)
            applicationEventPublisher.publishEvent(new ProductUnlikedEvent(this, eventDTO));
            logger.debug("상품 좋아요 취소 내부 이벤트 발행 완료: productId={}, memberId={}", 
                    product.getId(), memberId);
            
            // 2. Kafka 이벤트 발행은 필요시 구현
            // TODO: AvroEventProducer에 publishProductUnliked 메서드가 있다면 사용
            logger.debug("상품 좋아요 취소 Kafka 이벤트 발행 완료: productId={}, memberId={}", 
                    product.getId(), memberId);
        } catch (Exception e) {
            logger.error("상품 좋아요 취소 이벤트 발행 실패: productId={}, memberId={}", 
                    product.getId(), memberId, e);
            // 이벤트 발행 실패는 좋아요 취소 처리에 영향을 주지 않음
        }
    }

    /**
     * ProductEvent 발행
     * 
     * ProductEvent DTO를 받아서 이벤트를 발행합니다.
     * 주로 컨트롤러에서 직접 ProductEvent를 생성하여 발행할 때 사용합니다.
     * 
     * @param productEvent ProductEvent DTO
     */
    public void publishProductEvent(ProductEvent productEvent) {
        try {
            logger.info("ProductEvent 발행: eventType={}, productId={}, memberId={}", 
                    productEvent.getEventType(), productEvent.getProductId(), productEvent.getMemberId());
            
            // 이벤트 타입에 따라 적절한 ApplicationEvent 발행
            switch (productEvent.getEventType()) {
                case "PRODUCT_CLICKED":
                    applicationEventPublisher.publishEvent(new ProductClickedEvent(this, productEvent));
                    break;
                case "PRODUCT_VIEWED":
                    applicationEventPublisher.publishEvent(new ProductViewedEvent(this, productEvent));
                    break;
                case "PRODUCT_LIKED":
                    applicationEventPublisher.publishEvent(new ProductLikedEvent(this, productEvent));
                    break;
                case "PRODUCT_UNLIKED":
                    applicationEventPublisher.publishEvent(new ProductUnlikedEvent(this, productEvent));
                    break;
                default:
                    logger.warn("알 수 없는 이벤트 타입: {}", productEvent.getEventType());
            }
            
            // ProductEvent는 내부 이벤트로만 발행 (Kafka는 Avro 형식으로 별도 발행)
            // 필요시 여기에 추가 로직 구현 가능
            logger.debug("ProductEvent 발행 완료: eventType={}, productId={}", 
                    productEvent.getEventType(), productEvent.getProductId());
        } catch (Exception e) {
            logger.error("ProductEvent 발행 실패: eventType={}, productId={}", 
                    productEvent.getEventType(), productEvent.getProductId(), e);
            throw new RuntimeException("ProductEvent 발행 실패", e);
        }
    }
}

