package com.talktrip.talktrip.domain.messaging.dto.product;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * 상품 이벤트 DTO
 * 
 * 상품 관련 모든 이벤트를 표현하는 일반적인 DTO입니다.
 * 이벤트 타입에 따라 다른 필드가 사용될 수 있습니다.
 * 
 * 이벤트 타입:
 * - PRODUCT_CLICKED: 상품 클릭
 * - PRODUCT_VIEWED: 상품 조회
 * - PRODUCT_LIKED: 상품 좋아요
 * - PRODUCT_UNLIKED: 상품 좋아요 취소
 * - PRODUCT_SEARCHED: 상품 검색
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductEvent {

    /**
     * 이벤트 타입
     * PRODUCT_CLICKED, PRODUCT_VIEWED, PRODUCT_LIKED, PRODUCT_UNLIKED, PRODUCT_SEARCHED 등
     */
    private String eventType;
    
    /**
     * 상품 ID
     */
    private Long productId;
    
    /**
     * 회원 ID (nullable, 비회원인 경우 null)
     */
    private Long memberId;
    
    /**
     * 이벤트 발생 시각 (epoch milliseconds)
     */
    private Long eventTimestamp;
    
    /**
     * 이벤트 발생 시각 (LocalDateTime)
     */
    private LocalDateTime eventDateTime;
    
    /**
     * 검색 키워드 (PRODUCT_SEARCHED 이벤트의 경우)
     */
    private String searchKeyword;
    
    /**
     * 검색 결과 수 (PRODUCT_SEARCHED 이벤트의 경우)
     */
    private Integer searchResultCount;
    
    /**
     * 상품 옵션 ID (특정 옵션 클릭 시)
     */
    private Long productOptionId;
    
    /**
     * 추가 메타데이터 (JSON 형식 또는 Map)
     */
    private String metadata;
    
    /**
     * 상품 클릭 이벤트 생성
     * 
     * @param productId 상품 ID
     * @param memberId 회원 ID (nullable)
     * @return ProductEvent
     */
    public static ProductEvent productClicked(Long productId, Long memberId) {
        LocalDateTime now = LocalDateTime.now();
        return ProductEvent.builder()
                .eventType("PRODUCT_CLICKED")
                .productId(productId)
                .memberId(memberId)
                .eventTimestamp(now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .eventDateTime(now)
                .build();
    }
    
    /**
     * 상품 조회 이벤트 생성
     * 
     * @param productId 상품 ID
     * @param memberId 회원 ID (nullable)
     * @return ProductEvent
     */
    public static ProductEvent productViewed(Long productId, Long memberId) {
        LocalDateTime now = LocalDateTime.now();
        return ProductEvent.builder()
                .eventType("PRODUCT_VIEWED")
                .productId(productId)
                .memberId(memberId)
                .eventTimestamp(now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .eventDateTime(now)
                .build();
    }
    
    /**
     * 상품 좋아요 이벤트 생성
     * 
     * @param productId 상품 ID
     * @param memberId 회원 ID
     * @return ProductEvent
     */
    public static ProductEvent productLiked(Long productId, Long memberId) {
        LocalDateTime now = LocalDateTime.now();
        return ProductEvent.builder()
                .eventType("PRODUCT_LIKED")
                .productId(productId)
                .memberId(memberId)
                .eventTimestamp(now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .eventDateTime(now)
                .build();
    }
    
    /**
     * 상품 좋아요 취소 이벤트 생성
     * 
     * @param productId 상품 ID
     * @param memberId 회원 ID
     * @return ProductEvent
     */
    public static ProductEvent productUnliked(Long productId, Long memberId) {
        LocalDateTime now = LocalDateTime.now();
        return ProductEvent.builder()
                .eventType("PRODUCT_UNLIKED")
                .productId(productId)
                .memberId(memberId)
                .eventTimestamp(now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .eventDateTime(now)
                .build();
    }
    
    /**
     * 상품 검색 이벤트 생성
     * 
     * @param searchKeyword 검색 키워드
     * @param memberId 회원 ID (nullable)
     * @param searchResultCount 검색 결과 수
     * @return ProductEvent
     */
    public static ProductEvent productSearched(String searchKeyword, Long memberId, Integer searchResultCount) {
        LocalDateTime now = LocalDateTime.now();
        return ProductEvent.builder()
                .eventType("PRODUCT_SEARCHED")
                .memberId(memberId)
                .searchKeyword(searchKeyword)
                .searchResultCount(searchResultCount)
                .eventTimestamp(now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                .eventDateTime(now)
                .build();
    }
}

