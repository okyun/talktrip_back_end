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
import org.apache.kafka.streams.kstream.Consumed;
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
 * - 이 클래스는 전통적인 @KafkaListener 기반 Consumer 대신,
 *   Kafka Streams를 사용해서 `order-created` 토픽을 "소비(consume)"하고 처리하는 역할을 합니다.
 *
 * - 즉, KafkaEventConsumer 같은 별도 Consumer 클래스 없이
 *   이 Processor가 곧 Kafka Consumer + 집계 로직을 모두 담당합니다.
 *
 * 기능 요약:
 * 1. 주문 생성 이벤트를 15분 간격으로 집계하여 TOP 30을 추출합니다.
 * 2. 0시부터 시작하여 15분 간격으로 윈도우를 생성합니다.
 * 3. 애플리케이션이 시작될 때(Spring이 빈 만들 때) 한 번만 실행되고,
 *    그 결과로 만들어진 토폴로지를 기반으로
 *    KafkaStreams가 계속 메시지를 감시/처리하는 구조입니다.
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
     * ⚠ 이 메서드는 Kafka Streams 토폴로지를 구성하면서,
     *   내부적으로는 `order-created` 토픽을 "구독/소비"하는 Consumer 역할까지 함께 수행합니다.
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
        // 키: orderId (String), 값: OrderCreatedEventDTO (JSON)
        JsonSerde<OrderCreatedEventDTO> orderCreatedEventSerde = createJsonSerde(OrderCreatedEventDTO.class);
        KStream<String, OrderCreatedEventDTO> orderStream = streamsBuilder.stream(
                orderCreatedTopic,
                Consumed.with(Serdes.String(), orderCreatedEventSerde)
        );

        // 주문 구매 통계 스트림 처리
        orderPurchaseStatsStream(orderStream);
        // TODO: 이상 거래(fraud) 탐지를 위한 추가 스트림 처리
        // fraudStream(orderStream);

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
     * 2. groupByKey: 상품 ID를 키로 그룹화
     * 3. windowedBy: 15분 간격 타임 윈도우로 집계
     * 4. count: 각 윈도우별 상품 구매 수 집계
     * 5. map: WindowedKey와 count를 OrderPurchaseStatResponse로 변환
     * 6. groupByKey: 윈도우 시작 시간을 키로 그룹화하여 같은 윈도우의 데이터를 묶음
     * 7. aggregate: 각 윈도우별로 TOP 30 상품 추출 및 State Store에 저장
     * 8. to: 결과를 order-purchase-stats 토픽으로 전송
     * 
     * ⚠️ 이 스트림은 State Store에 집계 결과를 유지하고 토픽으로도 전송합니다.
     *    나중에 API 레이어에서 이 상태 스토어를 조회해서 "실시간 주문 구매 통계"로 활용할 수 있습니다.
     * 
     * @param orderStream 주문 생성 이벤트 스트림 (OrderCreatedEventDTO)
     */
    private void orderPurchaseStatsStream(KStream<String, OrderCreatedEventDTO> orderStream) {
        // 주문 이벤트에서 productId를 추출하여 그룹화
        // 하나의 주문에 여러 아이템이 있을 수 있으므로 flatMap을 사용하여 여러 레코드로 변환
        KStream<String, String> productIdStream = orderStream
                .flatMap((key, orderEvent) -> {
                    List<KeyValue<String, String>> result = new ArrayList<>();

                    try {
                        if (orderEvent != null && orderEvent.getItems() != null) {
                            for (OrderItemEventDTO item : orderEvent.getItems()) {
                                if (item.getProductId() != null) {
                                    String productId = String.valueOf(item.getProductId());
                                    // 각 아이템의 수량만큼 레코드 생성 (구매 수 집계를 위해)
                                    int qty = item.getQuantity() != null ? item.getQuantity() : 1;
                                    for (int i = 0; i < qty; i++) {
                                        result.add(KeyValue.pair(productId, productId));
                                    }
                                    logger.debug("주문 이벤트에서 productId 추출: orderId={}, productId={}, quantity={}",
                                            orderEvent.getOrderId(), productId, item.getQuantity());
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error("주문 이벤트에서 productId 추출 실패: key={}, value={}", key, orderEvent, e);
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

    /**
     * 이상 거래(Fraud) 탐지용 fraudStream (스켈레톤)
     *
     * 이 메서드에는 다음과 같은 로직을 추가할 수 있습니다.
     * - 짧은 시간 안에 동일 memberId 가 많은 주문을 발생시키는 패턴 탐지
     * - 비정상적으로 큰 금액/빈도의 주문 감지
     * - 특정 productId 에 대한 비정상적인 구매 패턴 탐지 등
     *
     * 현재는 구조만 정의해두고, 실제 비즈니스 규칙은 추후 구현을 위해 남겨둡니다.
     */
    @SuppressWarnings("unused")
    private void fraudStream(KStream<String, OrderCreatedEventDTO> orderStream) {
        // 예시: 기본 골격 (실제 로직은 추후 구현)
        // TimeWindows fraudWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5));
        //
        // orderStream
        //     .groupBy((key, event) -> String.valueOf(event.getMemberId()), Grouped.with(Serdes.String(), orderCreatedEventSerde))
        //     .windowedBy(fraudWindow)
        //     .count()
        //     .toStream()
        //     .filter((windowedKey, count) -> count > SOME_THRESHOLD)
        //     .peek((windowedKey, count) -> logger.warn("잠재적 이상 주문 감지: memberId={}, count={}", windowedKey.key(), count));
    }
}

