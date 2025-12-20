package com.talktrip.talktrip.domain.messaging.dto.order;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 사기 의심 알림 DTO
 * 
 * 이상 거래(사기 의심)를 탐지했을 때, 알림 이벤트로 사용하는 모델입니다.
 * Kafka Streams에서 고액 주문이나 이상 패턴을 감지했을 때 발행됩니다.
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FraudAlert {
    
    /**
     * 의심 주문 ID
     */
    private String orderId;
    
    /**
     * 고객 ID
     */
    private String customerId;
    
    /**
     * 사기 의심 사유
     * 예: "단시간 다수 결제", "고액 주문", "대량 주문" 등
     */
    private String reason;
    
    /**
     * 심각도 (LOW ~ CRITICAL)
     */
    private FraudSeverity severity;
    
    /**
     * 알림 생성 시간
     * 기본값: 현재 시각
     * JSON 직렬화 시 "yyyy-MM-dd'T'HH:mm:ss" 형식으로 변환
     */
    @Builder.Default
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime timestamp = LocalDateTime.now();
}

