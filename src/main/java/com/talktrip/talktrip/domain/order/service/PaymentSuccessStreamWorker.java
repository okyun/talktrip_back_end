package com.talktrip.talktrip.domain.order.service;

import com.talktrip.talktrip.domain.order.entity.Order;
import com.talktrip.talktrip.domain.order.entity.Payment;
import com.talktrip.talktrip.domain.order.enums.OrderStatus;
import com.talktrip.talktrip.domain.order.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Redis Stream (stream:payment:success) 을 읽어
 * OrderService.processSuccessfulPayment 를 실행하는 워커.
 *
 * - Consumer Group: payment-success-workers
 * - Consumer Name: worker-1 (인스턴스별로 다르게 설정 가능)
 *
 * 멱등 보장을 위해:
 * - 동일 orderCode/paymentKey 로 이미 SUCCESS + Payment 가 붙어 있으면 스킵 후 ACK.
 */
@Service
@RequiredArgsConstructor
public class PaymentSuccessStreamWorker {

    private static final Logger logger = LoggerFactory.getLogger(PaymentSuccessStreamWorker.class);

    private static final String STREAM_KEY = "stream:payment:success";
    private static final String GROUP = "payment-success-workers";
    private static final String CONSUMER = "worker-1";

    private final StringRedisTemplate stringRedisTemplate;
    private final OrderRepository orderRepository;
    private final OrderService orderService;

    @PostConstruct
    public void initGroup() {
        try {
            // 이미 존재하면 예외가 발생하므로, 실패 로그만 남기고 지나간다.
            stringRedisTemplate.opsForStream()
                    .createGroup(STREAM_KEY, ReadOffset.latest(), GROUP);
            logger.info("Redis Stream Consumer Group 생성: stream={}, group={}", STREAM_KEY, GROUP);
        } catch (Exception e) {
            logger.info("Redis Stream Consumer Group 생성 생략 (이미 존재 가능): {}", e.getMessage());
        }
    }

    /**
     * 주기적으로 스트림에서 결제 성공 이벤트를 읽어 처리한다.
     * - fixedDelay: 이전 실행 종료 후 2초 뒤에 다시 실행
     */
    @Scheduled(fixedDelay = 2000L)
    @SuppressWarnings("unchecked")
    public void pollAndProcess() {
        try {
            List<MapRecord<String, String, String>> records =
                    stringRedisTemplate.<String, String>opsForStream().read(
                            Consumer.from(GROUP, CONSUMER),
                            StreamReadOptions.empty()
                                    .count(10)
                                    .block(java.time.Duration.ofSeconds(1)),
                            StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed())
                    );

            if (records == null || records.isEmpty()) {
                return;
            }

            for (MapRecord<String, String, String> record : records) {
                processRecord(record);
            }
        } catch (Exception e) {
            logger.error("payment-success 스트림 읽기 실패: {}", e.getMessage(), e);
        }
    }

    private void processRecord(MapRecord<String, String, String> record) {
        RecordId id = record.getId();
        Map<String, String> fields = record.getValue();

        String type = fields.get("type");
        if (!"PAYMENT_SUCCESS".equals(type)) {
            acknowledge(id);
            return;
        }

        String orderCode = fields.get("orderCode");
        String paymentKey = fields.get("paymentKey");
        String payload = fields.get("payload");

        try {
            if (isAlreadyProcessed(orderCode, paymentKey)) {
                logger.info("이미 처리된 결제 성공 이벤트: orderCode={}, paymentKey={}", orderCode, paymentKey);
                acknowledge(id);
                return;
            }

            JSONObject responseJson = parsePayload(payload);
            Order order = findOrderByOrderCode(orderCode);

            orderService.processSuccessfulPayment(order, responseJson);

            acknowledge(id);
        } catch (Exception e) {
            logger.error("결제 성공 이벤트 처리 실패: streamId={}, orderCode={}, error={}",
                    id.getValue(), orderCode, e.getMessage(), e);
            // ACK 하지 않으면 pending 으로 남아 나중에 재시도 가능
        }
    }

    private void acknowledge(RecordId id) {
        try {
            stringRedisTemplate.opsForStream().acknowledge(STREAM_KEY, GROUP, id);
        } catch (Exception e) {
            logger.warn("Stream ACK 실패: id={}, error={}", id.getValue(), e.getMessage());
        }
    }

    private boolean isAlreadyProcessed(String orderCode, String paymentKey) {
        Optional<Order> optionalOrder = orderRepository.findByOrderCode(orderCode);
        if (optionalOrder.isEmpty()) {
            return false;
        }

        Order order = optionalOrder.get();
        Payment payment = order.getPayment();

        return payment != null
                && paymentKey != null
                && paymentKey.equals(payment.getPaymentKey())
                && order.getOrderStatus() == OrderStatus.SUCCESS;
    }

    private Order findOrderByOrderCode(String orderCode) {
        return orderRepository.findByOrderCode(orderCode)
                .orElseThrow(() -> new IllegalStateException("orderCode 에 해당하는 주문이 없습니다: " + orderCode));
    }

    private JSONObject parsePayload(String payload) throws ParseException {
        JSONParser parser = new JSONParser();
        return (JSONObject) parser.parse(payload);
    }
}

