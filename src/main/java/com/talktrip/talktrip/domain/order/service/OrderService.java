package com.talktrip.talktrip.domain.order.service;

import com.talktrip.talktrip.domain.member.entity.Member;
import com.talktrip.talktrip.domain.member.repository.MemberRepository;
import com.talktrip.talktrip.domain.order.dto.request.OrderRequestDTO;
import com.talktrip.talktrip.domain.order.dto.response.OrderDetailResponseDTO;
import com.talktrip.talktrip.domain.order.dto.response.OrderHistoryResponseDTO;
import com.talktrip.talktrip.domain.order.dto.response.OrderResponseDTO;
import com.talktrip.talktrip.domain.order.entity.CardPayment;
import com.talktrip.talktrip.domain.order.entity.Order;
import com.talktrip.talktrip.domain.order.entity.OrderItem;
import com.talktrip.talktrip.domain.order.entity.Payment;
import com.talktrip.talktrip.domain.order.enums.OrderStatus;
import com.talktrip.talktrip.domain.order.enums.PaymentMethod;
import com.talktrip.talktrip.domain.order.enums.PaymentProvider;
import com.talktrip.talktrip.domain.order.repository.CardPaymentRepository;
import com.talktrip.talktrip.domain.order.repository.OrderRepository;
import com.talktrip.talktrip.domain.order.repository.PaymentRepository;
import com.talktrip.talktrip.domain.product.entity.Product;
import com.talktrip.talktrip.domain.product.entity.ProductOption;
import com.talktrip.talktrip.domain.product.repository.ProductRepository;
import com.talktrip.talktrip.domain.product.service.StockService;
import com.talktrip.talktrip.domain.event.order.OrderEventPublisher;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Transactional
public class OrderService {

    private static final Logger logger = LoggerFactory.getLogger(OrderService.class);

    private final ProductRepository productRepository;
    private final MemberRepository memberRepository;
    private final OrderRepository orderRepository;
    private final PaymentRepository paymentRepository;
    private final CardPaymentRepository cardPaymentRepository;
    private final OrderEventPublisher orderEventPublisher;
    private final StockService stockService;

    public OrderResponseDTO createOrder(Long productId, OrderRequestDTO orderRequest, Long memberId) {

        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new IllegalArgumentException("상품이 존재하지 않습니다."));

        Member member = memberRepository.findById(memberId)
                .orElseThrow(() -> new IllegalArgumentException("회원이 존재하지 않습니다."));

        List<OrderItem> orderItems = orderRequest.getOptions().stream()
                .map(optReq -> {
                    // 재고 확인 및 차감 (StockService를 통해 처리)
                    ProductOption productOption = stockService.checkStockAndDecrease(
                            optReq.getProductOptionId(), 
                            optReq.getQuantity()
                    );

                    // 스냅샷으로 OrderItem 생성
                    return OrderItem.createOrderItem(
                            product.getId(),
                            product.getProductName(),
                            product.getThumbnailImageUrl(),
                            product.getMinPriceOption() != null ? product.getMinPriceOption().getDiscountPrice() : product.getMinPriceOption().getPrice(),
                            productOption.getId(),
                            productOption.getOptionName(),
                            productOption.getPrice(),
                            productOption.getDiscountPrice(),
                            productOption.getStartDate(),
                            optReq.getQuantity(),
                            optReq.getPrice() // 프론트엔드에서 전달받은 실제 결제 가격
                    );
                })
                .collect(Collectors.toList());

        int totalQuantity = orderItems.stream().mapToInt(OrderItem::getQuantity).sum();
        String orderName = (totalQuantity == 1)
                ? product.getProductName() + " - 단일 옵션"
                : product.getProductName() + " 외 " + (totalQuantity - 1) + "건";

        Order order = Order.createOrder(
                member,
                LocalDate.parse(orderRequest.getDate()),
                orderRequest.getTotalPrice()
        );

        order.setOrderCode(generateTossOrderId());

        orderItems.forEach(order::addOrderItem);

        orderRepository.save(order);

        // 주문 생성 이벤트 발행 (Avro 형식)
        try {
            orderEventPublisher.publishOrderCreated(order);
            logger.info("주문 생성 이벤트 발행 완료: orderId={}, orderCode={}", order.getId(), order.getOrderCode());
        } catch (Exception e) {
            logger.error("주문 생성 이벤트 발행 실패: orderId={}, orderCode={}", order.getId(), order.getOrderCode(), e);
            // 이벤트 발행 실패는 주문 생성 자체를 롤백하지 않음 (비동기 처리)
        }

        return new OrderResponseDTO(
                order.getOrderCode(),
                orderName,
                order.getTotalPrice(),
                member.getAccountEmail()
        );
    }

    private String generateTossOrderId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    public List<OrderHistoryResponseDTO> getOrdersByMemberId(Long memberId) {
        List<Order> orders = orderRepository.findByMemberIdAndOrderStatus(memberId, OrderStatus.SUCCESS);

        return orders.stream()
                .map(order -> {
                    if (order.getPayment() == null) {
                        throw new IllegalStateException("결제 정보가 없는 주문이 존재합니다. Order ID: " + order.getId() + ", Order Code: " + order.getOrderCode());
                    }
                    return OrderHistoryResponseDTO.fromEntity(order);
                })
                .collect(Collectors.toList());
    }

    // 페이지네이션을 지원하는 새로운 메서드
    public Page<OrderHistoryResponseDTO> getOrdersByMemberIdWithPagination(Long memberId, Pageable pageable) {
        // SUCCESS 상태의 주문만 조회 (결제 완료된 주문만)
        Page<Order> orderPage = orderRepository.findByMemberIdAndOrderStatus(memberId, OrderStatus.SUCCESS, pageable);

        return orderPage.map(order -> {
            if (order.getPayment() == null) {
                throw new IllegalStateException("결제 정보가 없는 주문이 존재합니다. Order ID: " + order.getId() + ", Order Code: " + order.getOrderCode());
            }
            return OrderHistoryResponseDTO.fromEntity(order);
        });
    }

    public void processSuccessfulPayment(Order order, JSONObject responseJson) {
        
        // 1. 공통 결제 정보 추출
        String paymentKey = (String) responseJson.get("paymentKey");
        String methodStr = (String) responseJson.get("method");
        String status = (String) responseJson.get("status");
        int totalAmount = ((Long) responseJson.get("totalAmount")).intValue();
        int vat = ((Long) responseJson.get("vat")).intValue();
        int suppliedAmount = ((Long) responseJson.get("suppliedAmount")).intValue();
        String receiptUrl = (String) responseJson.get("receiptUrl");
        boolean isPartialCancelable = (Boolean) responseJson.get("isPartialCancelable");

        String approvedAtStr = (String) responseJson.get("approvedAt");
        LocalDateTime approvedAt = LocalDateTime.parse(approvedAtStr, DateTimeFormatter.ISO_DATE_TIME);

        // 2. 결제 수단 및 제공자 매핑
        PaymentMethod paymentMethod = mapToPaymentMethod(methodStr);
        PaymentProvider paymentProvider = mapToPaymentProvider(responseJson);

        // 3. 상세 결제 정보 추출
        String easyPayProvider = null;
        String cardCompany = null;
        String accountBank = null;

        if ("카드".equals(methodStr) && responseJson.containsKey("card")) {
            JSONObject cardJson = (JSONObject) responseJson.get("card");
            if (cardJson != null) {
                cardCompany = (String) cardJson.get("issuerCode");
            }
        } else if ("간편결제".equals(methodStr) && responseJson.containsKey("easyPay")) {
            JSONObject easyPayJson = (JSONObject) responseJson.get("easyPay");
            if (easyPayJson != null) {
                easyPayProvider = (String) easyPayJson.get("provider");
            }
        } else if ("계좌이체".equals(methodStr) && responseJson.containsKey("transfer")) {
            JSONObject transferJson = (JSONObject) responseJson.get("transfer");
            if (transferJson != null) {
                accountBank = (String) transferJson.get("bank");
            }
        }

        // 4. Payment 생성 및 저장
        Payment payment = Payment.createPayment(
                order,
                paymentKey,
                paymentMethod,
                paymentProvider,
                totalAmount,
                vat,
                suppliedAmount,
                status,
                approvedAt,
                receiptUrl,
                isPartialCancelable,
                easyPayProvider,
                cardCompany,
                accountBank
        );
        
        payment = paymentRepository.save(payment);

        // 5. 카드 결제인 경우 CardPayment 추가 및 저장
        if ("카드".equals(methodStr) && responseJson.containsKey("card")) {
            JSONObject cardJson = (JSONObject) responseJson.get("card");
            if (cardJson != null) {
                System.out.println("카드 정보: " + cardJson.toJSONString());
                
                CardPayment cardPayment = CardPayment.createCardPayment(
                        payment,
                        (String) cardJson.get("number"),
                        (String) cardJson.get("issuerCode"),
                        (String) cardJson.get("acquirerCode"),
                        (String) cardJson.get("approveNo"),
                        ((Long) cardJson.get("installmentPlanMonths")).intValue(),
                        (Boolean) cardJson.get("isInterestFree"),
                        (String) cardJson.get("cardType"),
                        (String) cardJson.get("ownerType"),
                        (String) cardJson.get("acquireStatus"),
                        totalAmount
                );

                cardPayment = cardPaymentRepository.save(cardPayment);
                payment.setCardPayment(cardPayment);
            }
        }

        // 6. 주문에 결제 정보 등록 및 상태 업데이트
        order.attachPayment(payment);
        order.updatePaymentInfo(paymentMethod, OrderStatus.SUCCESS);

        // 7. 재고 차감은 이미 주문 생성 시점에 처리되었으므로 여기서는 제거
        // (스냅샷 패턴에서는 주문 생성 시점에 재고를 차감하고, 취소 시에만 복원)

        orderRepository.save(order);

        // 8. 결제 성공 이벤트 발행
        try {
            orderEventPublisher.publishPaymentSuccess(order, payment);
            logger.info("결제 성공 이벤트 발행 완료: orderId={}, orderCode={}, paymentKey={}", 
                    order.getId(), order.getOrderCode(), payment.getPaymentKey());
        } catch (Exception e) {
            logger.error("결제 성공 이벤트 발행 실패: orderId={}, orderCode={}", 
                    order.getId(), order.getOrderCode(), e);
            // 이벤트 발행 실패는 결제 처리에 영향을 주지 않음
        }
    }

    private PaymentMethod mapToPaymentMethod(String methodStr) {
        if (methodStr == null) return PaymentMethod.UNKNOWN;

        switch (methodStr) {
            case "카드":
                return PaymentMethod.CARD;
            case "계좌이체":
                return PaymentMethod.ACCOUNT;
            case "휴대폰결제":
                return PaymentMethod.MOBILE;
            case "가상계좌":
                return PaymentMethod.VIRTUAL_ACCOUNT;
            case "간편결제":
                return PaymentMethod.EASY_PAY;
            default:
                return PaymentMethod.UNKNOWN;
        }
    }

    private PaymentProvider mapToPaymentProvider(JSONObject responseJson) {
        if (responseJson.containsKey("easyPay")) {
            JSONObject easyPay = (JSONObject) responseJson.get("easyPay");
            
            // easyPay 객체가 null인 경우 처리
            if (easyPay == null) {
                return PaymentProvider.UNKNOWN;
            }
            
            String provider = (String) easyPay.get("provider");

            if (provider == null) return PaymentProvider.UNKNOWN;

            switch (provider.toLowerCase()) {
                case "토스페이":
                    return PaymentProvider.TOSSPAY;
                case "카카오페이":
                    return PaymentProvider.KAKAO;
                case "페이코":
                    return PaymentProvider.PAYCO;
                case "네이버페이":
                    return PaymentProvider.NAVER;
                default:
                    return PaymentProvider.UNKNOWN;
            }
        }
        
        // easyPay가 아닌 경우 (카드, 계좌이체 등)
        return PaymentProvider.UNKNOWN;
    }

    // 주문 취소 메서드(미리 구현한 것뿐. 아직 사용 x)
    public void cancelOrder(Long orderId, Long requesterId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new EntityNotFoundException("주문을 찾을 수 없습니다."));

        if (!order.getMember().getId().equals(requesterId)) {
            throw new AccessDeniedException("해당 주문에 접근할 수 없습니다.");
        }

        // 재고 복원 (스냅샷 패턴에서는 취소 시에만 재고를 복원)
        for (OrderItem item : order.getOrderItems()) {
            stockService.restoreStock(item.getProductOptionId(), item.getQuantity());
        }

        order.cancel();
    }

    public OrderDetailResponseDTO getOrderDetail(Long orderId, Long requesterId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new EntityNotFoundException("주문을 찾을 수 없습니다."));

        if (!order.getMember().getId().equals(requesterId)) {
            throw new AccessDeniedException("해당 주문에 접근할 수 없습니다.");
        }

        if (order.getPayment() == null) {
            throw new IllegalStateException("결제 정보가 누락된 주문입니다.");
        }

        return OrderDetailResponseDTO.from(order);
    }

}
