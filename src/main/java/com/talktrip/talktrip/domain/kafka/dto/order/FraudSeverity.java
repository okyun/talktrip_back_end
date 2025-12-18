package com.talktrip.talktrip.domain.kafka.dto.order;

/**
 * 사기 의심 알림의 심각도 단계
 * 
 * 이상 거래(사기 의심)를 탐지했을 때의 위험 수준을 나타냅니다.
 */
public enum FraudSeverity {
    
    /**
     * 낮은 위험
     */
    LOW,
    
    /**
     * 중간 위험
     */
    MEDIUM,
    
    /**
     * 높은 위험
     */
    HIGH,
    
    /**
     * 매우 위험
     */
    CRITICAL
}

