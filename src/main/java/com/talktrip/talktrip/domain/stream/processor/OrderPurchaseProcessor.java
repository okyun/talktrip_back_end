package com.talktrip.talktrip.domain.stream.processor;

import com.talktrip.talktrip.domain.messaging.dto.order.OrderCreatedEventDTO;
import com.talktrip.talktrip.domain.messaging.dto.order.OrderItemEventDTO;
import com.talktrip.talktrip.domain.messaging.dto.order.OrderPurchaseStatResponse;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 주문 구매 통계 Processor
 * 
 * 주문 생성 이벤트를 15분 간격으로 집계하여 TOP 30을 추출합니다.
 * 0시부터 시작하여 15분 간격으로 윈도우를 생성합니다.
 * 
 * 애플리케이션이 시작될 때(Spring이 빈 만들 때) 한 번만 실행되고,
 * 그 결과로 만들어진 토폴로지를 기반으로
 * KafkaStreams가 계속 메시지를 감시/처리하는 구조입니다.
 */
@Component
public class OrderPurchaseProcessor {

    private static final Logger logger = LoggerFactory.getLogger(OrderPurchaseProcessor.class);

    // 주문 생성 이벤트가 들어오는 원본 토픽
    @Value("${kafka.topics.order-created:order-created}")
    private String orderCreatedTopic;

    // 주문 구매 통계 결과를 보내는 토픽
    @Value("${kafka.topics.order-purchase-stats:order-purchase-stats}")
    private String orderPurchaseStatsTopic;

    private static final Duration WINDOW_SIZE = Duration.ofMinutes(15);
    private static final int TOP_N = 30;

    // JsonSerde - Kafka Stream에서만 사용되는 직렬화, 역직렬화의 줄인말
    private final JsonSerde<OrderPurchaseStatResponse> orderPurchaseStatSerde = createJsonSerde(OrderPurchaseStatResponse.class);
    
    @SuppressWarnings("unchecked")
    private final JsonSerde<List<OrderPurchaseStatResponse>> orderPurchaseStatListSerde = 
            (JsonSerde<List<OrderPurchaseStatResponse>>) (JsonSerde<?>) createJsonSerde(List.class);

    /**
     * JsonSerde 생성 메서드
     * 
     * Kafka Streams에서 JSON 직렬화/역직렬화를 위한 JsonSerde를 생성합니다.
     * trusted.packages를 "*"로 설정하여 모든 패키지의 클래스를 허용하고,
     * type 정보를 헤더에 추가하지 않도록 설정합니다.
     * 
     * @param <T> 직렬화할 타입
     * @param clazz 직렬화할 클래스
     * @return 설정된 JsonSerde
     */
    private <T> JsonSerde<T> createJsonSerde(Class<T> clazz) {
        JsonSerde<T> serde = new JsonSerde<>(clazz);
        Map<String, Object> props = new HashMap<>();
        props.put("spring.json.trusted.packages", "*");
        props.put("spring.json.add.type.headers", false);
        props.put("spring.json.value.default.type", clazz.getName());
        serde.configure(props, false);
        return serde;
    }

    /**
     * 주문 구매 통계 처리 로직
     * 
     * 애플리케이션 시작 시 한 번만 실행되며, 다음과 같은 처리를 수행합니다:
     * 1. order-created 토픽에서 주문 생성 이벤트를 스트림으로 읽어옵니다.
     * 2. 주문 이벤트에서 각 주문 아이템의 productId를 추출합니다.
     * 3. 15분 간격 타임 윈도우로 상품별 구매 수를 집계합니다.
     * 4. 각 윈도우별로 구매 수가 많은 상위 30개 상품을 추출합니다.
     * 5. 결과를 order-purchase-stats 토픽으로 전송하고 State Store에 저장합니다.
     * 
     * State Store는 나중에 API 레이어에서 조회하여 "실시간 주문 구매 통계"로 활용할 수 있습니다.
     * 
     * @param streamsBuilder StreamsBuilder - Kafka Streams 토폴로지를 구성하는 빌더
     */
    public void process(StreamsBuilder streamsBuilder) {
        logger.info("OrderPurchaseProcessor Topology 구성 시작: inputTopic={}, outputTopic={}, windowSize={}, topN={}", 
                orderCreatedTopic, orderPurchaseStatsTopic, WINDOW_SIZE, TOP_N);

        // 입력 스트림: order-created 토픽에서 주문 이벤트를 받음
        // 키: orderId (String), 값: OrderCreatedEventDTO (JSON) 또는 GenericRecord (Avro)
        // Object 타입으로 받아서 다양한 형식(JSON, Avro)을 처리할 수 있도록 함
        KStream<String, Object> orderStream = streamsBuilder.stream(orderCreatedTopic);

        // 주문 구매 통계 스트림 처리
        orderPurchaseStatsStream(orderStream);

        logger.info("OrderPurchaseProcessor Topology 구성 완료");
    }

    /**
     * 주문 구매 통계 스트림 처리
     * 
     * 주문 생성 이벤트에서 productId를 추출하여 15분 간격 타임 윈도우로 집계하고,
     * 각 윈도우별로 TOP 30 상품을 추출하여 State Store에 저장하고 토픽으로 전송합니다.
     * 
     * 처리 과정:
     * 1. flatMap: 주문 이벤트에서 각 주문 아이템의 productId를 추출하여 여러 레코드로 변환
     *    - OrderCreatedEventDTO의 items에서 productId 추출
     *    - GenericRecord의 items에서 productId 추출 (Avro 형식 지원)
     * 2. groupByKey: 상품 ID를 키로 그룹화
     * 3. windowedBy: 15분 간격 타임 윈도우로 집계
     * 4. count: 각 윈도우별 상품 구매 수 집계
     * 5. map: WindowedKey와 count를 OrderPurchaseStatResponse로 변환
     * 6. groupByKey: 윈도우 시작 시간을 키로 그룹화하여 같은 윈도우의 데이터를 묶음
     * 7. aggregate: 각 윈도우별로 TOP 30 상품 추출 및 State Store에 저장
     * 8. to: 결과를 order-purchase-stats 토픽으로 전송
     * 
     * ⚠️ 이 스트림은 State Store에 집계 결과를 유지하고 토픽으로도 전송합니다.
     * → 나중에 API 레이어에서 이 상태 스토어를 조회해서 "실시간 주문 구매 통계"로 활용 가능
     * 
     * @param orderStream 주문 생성 이벤트 스트림
     */
    private void orderPurchaseStatsStream(KStream<String, Object> orderStream) {
        // 주문 이벤트에서 productId를 추출하여 그룹화
        // 하나의 주문에 여러 아이템이 있을 수 있으므로 flatMap을 사용하여 여러 레코드로 변환
        KStream<String, String> productIdStream = orderStream
                .flatMap((key, value) -> {
                    List<KeyValue<String, String>> result = new ArrayList<>();
                    
                    try {
                        // OrderCreatedEventDTO 형식 처리
                        if (value instanceof OrderCreatedEventDTO) {
                            OrderCreatedEventDTO orderEvent = (OrderCreatedEventDTO) value;
                            if (orderEvent.getItems() != null) {
                                for (OrderItemEventDTO item : orderEvent.getItems()) {
                                    if (item.getProductId() != null) {
                                        String productId = String.valueOf(item.getProductId());
                                        // 각 아이템의 수량만큼 레코드 생성 (구매 수 집계를 위해)
                                        for (int i = 0; i < (item.getQuantity() != null ? item.getQuantity() : 1); i++) {
                                            result.add(KeyValue.pair(productId, productId));
                                        }
                                        logger.debug("주문 이벤트에서 productId 추출: orderId={}, productId={}, quantity={}", 
                                                orderEvent.getOrderId(), productId, item.getQuantity());
                                    }
                                }
                            }
                        }
                        // GenericRecord 형식 처리 (Avro 지원)
                        else if (value instanceof org.apache.avro.generic.GenericRecord) {
                            org.apache.avro.generic.GenericRecord record = (org.apache.avro.generic.GenericRecord) value;
                            @SuppressWarnings("unchecked")
                            List<org.apache.avro.generic.GenericRecord> items = 
                                    (List<org.apache.avro.generic.GenericRecord>) record.get("items");
                            
                            if (items != null) {
                                for (org.apache.avro.generic.GenericRecord item : items) {
                                    Long productId = (Long) item.get("productId");
                                    Integer quantity = (Integer) item.get("quantity");
                                    if (productId != null) {
                                        String productIdStr = String.valueOf(productId);
                                        // 각 아이템의 수량만큼 레코드 생성
                                        int qty = quantity != null ? quantity : 1;
                                        for (int i = 0; i < qty; i++) {
                                            result.add(KeyValue.pair(productIdStr, productIdStr));
                                        }
                                        logger.debug("Avro 레코드에서 productId 추출: productId={}, quantity={}", 
                                                productId, qty);
                                    }
                                }
                            }
                        }
                        // JSON 문자열 형식 처리 (필요한 경우)
                        else if (value instanceof String) {
                            logger.warn("JSON 문자열 형식의 주문 이벤트는 아직 지원하지 않습니다. value={}", value);
                        }
                        else {
                            logger.warn("지원하지 않는 주문 이벤트 형식: {}", value != null ? value.getClass().getName() : "null");
                        }
                    } catch (Exception e) {
                        logger.error("주문 이벤트에서 productId 추출 실패: key={}, value={}", key, value, e);
                    }
                    
                    return result;
                });

        // 15분 간격 타임 윈도우로 집계
        // ofSizeWithNoGrace: 윈도우 크기만 지정하고 grace period 없음 (정확한 15분 간격)
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE);

        // 상품 ID를 키로 하여 15분 간격으로 구매 수 집계
        // 결과: KTable<Windowed<String>, Long>
        // - Windowed<String>: 윈도우 정보와 상품 ID를 포함한 키
        // - Long: 해당 윈도우에서의 구매 수
        KTable<Windowed<String>, Long> purchaseCounts = productIdStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(tumblingWindow)
                .count(Materialized.as("order-purchase-count-store"));  // ⭐ 여기서 RocksDB 생성 & 저장

        // TOP 30 추출 및 출력
        purchaseCounts
                .toStream()
                // WindowedKey와 count를 OrderPurchaseStatResponse로 변환
                .map((windowedKey, count) -> {
                    String productId = windowedKey.key();
                    Instant windowStart = Instant.ofEpochMilli(windowedKey.window().start());
                    Instant windowEnd = Instant.ofEpochMilli(windowedKey.window().end());
                    
                    logger.debug("주문 구매 통계: productId={}, count={}, windowStart={}, windowEnd={}", 
                            productId, count, windowStart, windowEnd);
                    
                    // OrderPurchaseStatResponse 생성
                    OrderPurchaseStatResponse stat = new OrderPurchaseStatResponse(
                            Long.parseLong(productId),
                            count,
                            windowStart,
                            windowEnd
                    );
                    
                    // 윈도우 시작 시간을 키로 사용하여 같은 윈도우의 데이터를 그룹화
                    // 예: "1704067200000" (2024-01-01 00:00:00의 epoch milliseconds)
                    String windowKey = String.valueOf(windowedKey.window().start());
                    return KeyValue.pair(windowKey, stat);
                })
                // 윈도우 시작 시간을 키로 그룹화하여 같은 윈도우의 모든 상품 통계를 묶음
                .groupByKey(Grouped.with(Serdes.String(), orderPurchaseStatSerde))
                // 각 윈도우별로 TOP 30 상품 추출
                .aggregate(
                        // 초기값: 빈 리스트
                        ArrayList::new,
                        // 집계 함수: 새로운 통계를 리스트에 추가하고 구매 수 기준으로 정렬하여 TOP 30 유지
                        (key, value, aggregate) -> {
                            aggregate.add(value);
                            // TOP 30 유지 (구매 수 기준 내림차순)
                            return aggregate.stream()
                                    .sorted(Comparator.comparing(OrderPurchaseStatResponse::purchaseCount).reversed())
                                    .limit(TOP_N)
                                    .collect(Collectors.toCollection(ArrayList::new));
                        },
                        // State Store 설정: order-purchase-top30-store에 저장
                        // ⭐ 여기서 RocksDB 생성 & 저장
                        Materialized.<String, List<OrderPurchaseStatResponse>, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("order-purchase-top30-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(orderPurchaseStatListSerde)
                )
                .toStream()
                // 결과를 order-purchase-stats 토픽으로 전송
                .to(orderPurchaseStatsTopic, Produced.with(Serdes.String(), orderPurchaseStatListSerde));
    }
}

