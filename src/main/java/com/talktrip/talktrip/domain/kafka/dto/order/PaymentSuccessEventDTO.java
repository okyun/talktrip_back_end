package com.talktrip.talktrip.domain.kafka.dto.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * 결제 성공 이벤트 DTO
 * Kafka로 발행되는 결제 성공 이벤트의 데이터 구조
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentSuccessEventDTO {
    private Long orderId;
    private String orderCode;
    
    // 공통 결제 정보
    private String paymentKey;
    private String method; // "카드", "계좌이체", "간편결제" 등
    private String status;
    private Integer totalAmount;
    private Integer vat;
    private Integer suppliedAmount;
    private String receiptUrl;
    private Boolean isPartialCancelable;
    private LocalDateTime approvedAt;
    
    // 상세 결제 정보 (카드, 간편결제, 계좌이체)
    private String easyPayProvider; // 카카오페이, 네이버페이, 페이코 등
    private String cardCompany; // 신한카드, 삼성카드 등
    private String accountBank; // 신한은행, 국민은행 등
    
    // 카드 결제 상세 정보 (카드 결제인 경우에만)
    private CardPaymentInfo cardPaymentInfo;
    
    /**
     * 카드 결제 상세 정보
     */
    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CardPaymentInfo {
        private String cardNumber;
        private String issuerCode;
        private String acquirerCode;
        private String approveNo;
        private Integer installmentPlanMonths;
        private Boolean isInterestFree;
        private String cardType; // 신용, 체크 등
        private String ownerType; // 개인, 법인
        private String acquireStatus;
    }
}

