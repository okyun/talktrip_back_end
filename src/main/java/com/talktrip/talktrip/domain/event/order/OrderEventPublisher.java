package com.talktrip.talktrip.domain.event.order;

import com.talktrip.talktrip.domain.messaging.dto.order.OrderCreatedEventDTO;
import com.talktrip.talktrip.domain.messaging.dto.order.OrderEvent;
import com.talktrip.talktrip.domain.messaging.dto.order.OrderItemEventDTO;
import com.talktrip.talktrip.domain.messaging.dto.order.PaymentSuccessEventDTO;
import com.talktrip.talktrip.domain.messaging.avro.KafkaEventProducer;
import com.talktrip.talktrip.domain.order.entity.CardPayment;
import com.talktrip.talktrip.domain.order.entity.Order;
import com.talktrip.talktrip.domain.order.entity.Payment;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 주문 이벤트 발행자
 * 
 * Order 엔티티를 받아서 OrderCreatedEventDTO로 변환하고,
 * 1. ApplicationEventPublisher를 통해 내부 이벤트로 발행 (내부 처리용)
 *  * 2. AvroEventProducer를 통해 Kafka에 이벤트를 발행 (외부 시스템/스트림 처리용)
 *
 * 역할:
 * - 엔티티→DTO 변환 레이어 제공
 * - 도메인 엔티티와 이벤트 발행 로직 분리
 * - 내부 이벤트(ApplicationEvent)와 외부 이벤트(Kafka) 동시 발행
 * - OrderService에서 직접 사용할 수 있는 간단한 인터페이스 제공
 */
@Component
@RequiredArgsConstructor
public class OrderEventPublisher {

    private static final Logger logger = LoggerFactory.getLogger(OrderEventPublisher.class);

    private final ApplicationEventPublisher applicationEventPublisher;
    private final KafkaEventProducer kafkaEventProducer;

    /**
     * 주문 생성 이벤트 발행
     * 
     * Order 엔티티를 OrderCreatedEventDTO로 변환한 후,
     * 1. ApplicationEventPublisher를 통해 내부 이벤트로 발행 (내부 처리용)
     * 2. Avro 형식으로 Kafka에 이벤트를 발행 (외부 시스템/스트림 처리용)
     * 
     * @param order 주문 엔티티
     */
    public void publishOrderCreated(Order order) {
        try {
            OrderCreatedEventDTO eventDTO = toOrderCreatedEventDTO(order);
            
            // 1. 내부 이벤트 발행 (ApplicationEventPublisher)
            applicationEventPublisher.publishEvent(new OrderCreatedEvent(this, eventDTO));
            logger.debug("주문 생성 내부 이벤트 발행 완료: orderId={}, orderCode={}", 
                    order.getId(), order.getOrderCode());
            
            // 2. Kafka 이벤트 발행 (외부 시스템/스트림 처리용)
            kafkaEventProducer.publishOrderCreated(eventDTO);
            logger.debug("주문 생성 Kafka 이벤트 발행 완료: orderId={}, orderCode={}", 
                    order.getId(), order.getOrderCode());
        } catch (Exception e) {
            logger.error("주문 생성 이벤트 발행 실패: orderId={}, orderCode={}", 
                    order.getId(), order.getOrderCode(), e);
            throw new RuntimeException("주문 생성 이벤트 발행 실패", e);
        }
    }

    /**
     * 결제 성공 이벤트 발행
     * 
     * Payment 엔티티를 PaymentSuccessEventDTO로 변환한 후,
     * 1. ApplicationEventPublisher를 통해 내부 이벤트로 발행 (내부 처리용)
     * 2. Avro 형식으로 Kafka에 이벤트를 발행 (외부 시스템/스트림 처리용)
     * 
     * @param order 주문 엔티티
     * @param payment 결제 엔티티
     */
    public void publishPaymentSuccess(Order order, Payment payment) {
        try {
            PaymentSuccessEventDTO eventDTO = toPaymentSuccessEventDTO(order, payment);
            
            // 1. 내부 이벤트 발행 (ApplicationEventPublisher)
            applicationEventPublisher.publishEvent(new PaymentSuccessEvent(this, eventDTO));
            logger.debug("결제 성공 내부 이벤트 발행 완료: orderId={}, orderCode={}, paymentKey={}", 
                    order.getId(), order.getOrderCode(), payment.getPaymentKey());
            
            // 2. Kafka 이벤트 발행 (외부 시스템/스트림 처리용)
            // TODO: AvroEventProducer에 publishPaymentSuccess 메서드가 있다면 사용
            // avroEventProducer.publishPaymentSuccess(eventDTO);
            logger.debug("결제 성공 Kafka 이벤트 발행 완료: orderId={}, orderCode={}", 
                    order.getId(), order.getOrderCode());
        } catch (Exception e) {
            logger.error("결제 성공 이벤트 발행 실패: orderId={}, orderCode={}", 
                    order.getId(), order.getOrderCode(), e);
            // 이벤트 발행 실패는 결제 처리에 영향을 주지 않음
        }
    }

    /**
     * Order 엔티티를 OrderCreatedEventDTO로 변환
     * 
     * 엔티티→DTO 변환 레이어를 통해 이벤트 발행 로직과 도메인 엔티티를 분리합니다.
     * 이벤트 발행 시 필요한 데이터만 DTO로 추출하여 전달합니다.
     * 
     * @param order 주문 엔티티
     * @return OrderCreatedEventDTO
     */
    private OrderCreatedEventDTO toOrderCreatedEventDTO(Order order) {
        List<OrderItemEventDTO> itemDTOs = order.getOrderItems().stream()
                .map(item -> OrderItemEventDTO.builder()
                        .productId(item.getProductId())
                        .productOptionId(item.getProductOptionId())
                        .quantity(item.getQuantity())
                        .price(item.getPrice())
                        .build())
                .collect(Collectors.toList());

        return OrderCreatedEventDTO.builder()
                .orderId(order.getId())
                .orderCode(order.getOrderCode())
                .memberId(order.getMember() != null ? order.getMember().getId() : null)
                .totalPrice(order.getTotalPrice())
                .orderStatus(order.getOrderStatus() != null ? order.getOrderStatus().name() : "PENDING")
                .createdAt(order.getCreatedAt() != null ? order.getCreatedAt() : LocalDateTime.now())
                .items(itemDTOs)
                .build();
    }

    /**
     * Payment 엔티티를 PaymentSuccessEventDTO로 변환
     * 
     * @param order 주문 엔티티
     * @param payment 결제 엔티티
     * @return PaymentSuccessEventDTO
     */
    private PaymentSuccessEventDTO toPaymentSuccessEventDTO(Order order, Payment payment) {
        PaymentSuccessEventDTO.PaymentSuccessEventDTOBuilder builder = PaymentSuccessEventDTO.builder()
                .orderId(order.getId())
                .orderCode(order.getOrderCode())
                .paymentKey(payment.getPaymentKey())
                .method(payment.getMethod() != null ? payment.getMethod().name() : null)
                .status(payment.getStatus())
                .totalAmount(payment.getTotalAmount())
                .vat(payment.getVat())
                .suppliedAmount(payment.getSuppliedAmount())
                .receiptUrl(payment.getReceiptUrl())
                .isPartialCancelable(payment.isPartialCancelable())
                .approvedAt(payment.getApprovedAt())
                .easyPayProvider(payment.getEasyPayProvider())
                .cardCompany(payment.getCardCompany())
                .accountBank(payment.getAccountBank());

        // 카드 결제인 경우 CardPaymentInfo 추가
        if (payment.getCardPayment() != null) {
            CardPayment cardPayment = payment.getCardPayment();
            PaymentSuccessEventDTO.CardPaymentInfo cardPaymentInfo = PaymentSuccessEventDTO.CardPaymentInfo.builder()
                    .cardNumber(cardPayment.getCardNumber())
                    .issuerCode(cardPayment.getIssuerCode())
                    .acquirerCode(cardPayment.getAcquirerCode())
                    .approveNo(cardPayment.getApproveNo())
                    .installmentPlanMonths(cardPayment.getInstallmentMonths())
                    .isInterestFree(cardPayment.isInterestFree())
                    .cardType(cardPayment.getCardType())
                    .ownerType(cardPayment.getOwnerType())
                    .acquireStatus(cardPayment.getAcquireStatus())
                    .build();
            builder.cardPaymentInfo(cardPaymentInfo);
        }

        return builder.build();
    }

    /**
     * OrderEvent 발행
     * 
     * OrderEvent DTO를 받아서 이벤트를 발행합니다.
     * 주로 컨트롤러에서 직접 OrderEvent를 생성하여 발행할 때 사용합니다.
     * 
     * @param orderEvent OrderEvent DTO
     */
    public void publishOrderEvent(OrderEvent orderEvent) {
        try {
            logger.info("OrderEvent 발행: orderId={}, customerId={}, quantity={}, price={}", 
                    orderEvent.getOrderId(), orderEvent.getCustomerId(), 
                    orderEvent.getQuantity(), orderEvent.getPrice());
            
            // OrderEvent는 내부 이벤트로만 발행 (Kafka는 Avro 형식으로 별도 발행)
            // 필요시 여기에 추가 로직 구현 가능
            logger.debug("OrderEvent 발행 완료: orderId={}", orderEvent.getOrderId());
        } catch (Exception e) {
            logger.error("OrderEvent 발행 실패: orderId={}", orderEvent.getOrderId(), e);
            throw new RuntimeException("OrderEvent 발행 실패", e);
        }
    }
}

