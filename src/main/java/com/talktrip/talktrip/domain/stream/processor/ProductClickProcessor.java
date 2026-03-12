package com.talktrip.talktrip.domain.stream.processor;

import com.talktrip.talktrip.domain.messaging.dto.product.ProductClickEventDTO;
import com.talktrip.talktrip.domain.messaging.dto.product.ProductClickStatResponse;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.serialization.Serdes;
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
 * 상품 클릭 통계 Processor
 *
 * - 이 클래스는 전통적인 @KafkaListener 기반 Consumer 대신,
 *   Kafka Streams를 사용해서 `product-click` 토픽을 "소비(consume)"하고 처리하는 역할을 합니다.
 *
 * - 즉, KafkaEventConsumer 같은 별도 Consumer 클래스 없이
 *   이 Processor가 곧 Kafka Consumer + 집계 로직을 모두 담당합니다.
 *
 * 기능 요약:
 * 1. 상품 클릭 이벤트를 15분 간격으로 집계하여 TOP 30을 추출합니다.
 * 2. 0시부터 시작하여 15분 간격으로 윈도우를 생성합니다.
 * 3. 애플리케이션이 시작될 때(Spring이 빈 만들 때) 한 번만 실행되고,
 *    그 결과로 만들어진 토폴로지를 기반으로
 *    KafkaStreams가 계속 메시지를 감시/처리하는 구조입니다.
 */
@Component
@RequiredArgsConstructor
public class ProductClickProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ProductClickProcessor.class);

    // 상품 클릭 이벤트가 들어오는 원본 토픽
    @Value("${kafka.topics.product-click:product-click}")
    private String productClickTopic;

    // 상품 클릭 통계 결과를 보내는 토픽
    @Value("${kafka.topics.product-click-stats:product-click-stats}")
    private String productClickStatsTopic;

    private static final Duration WINDOW_SIZE = Duration.ofMinutes(15);
    private static final int TOP_N = 30;

    // JsonSerde - Kafka Stream에서만 사용되는 직렬화, 역직렬화의 줄인말
    private final JsonSerde<ProductClickStatResponse> productClickStatSerde = createJsonSerde(ProductClickStatResponse.class);
    
    @SuppressWarnings("unchecked")
    private final JsonSerde<List<ProductClickStatResponse>> productClickStatListSerde = 
            (JsonSerde<List<ProductClickStatResponse>>) (JsonSerde<?>) createJsonSerde(List.class);

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
     * 상품 클릭 통계 처리 로직
     * 
     * ⚠ 이 메서드는 Kafka Streams 토폴로지를 구성하면서,
     *   내부적으로는 `product-click` 토픽을 "구독/소비"하는 Consumer 역할까지 함께 수행합니다.
     *
     * 애플리케이션 시작 시 한 번만 실행되며, 다음과 같은 처리를 수행합니다:
     * 1. product-click 토픽에서 JSON 형식의 상품 클릭 이벤트를 스트림으로 읽어옵니다.
     * 2. productId 를 기준으로 15분 간격 타임 윈도우로 클릭 수를 집계합니다.
     * 3. 각 윈도우별로 클릭 수가 많은 상위 30개 상품을 추출합니다.
     * 4. 결과를 product-click-stats 토픽으로 전송하고 State Store에 저장합니다.
     * 
     * State Store는 나중에 API 레이어에서 조회하여 "실시간 상품 클릭 통계"로 활용할 수 있습니다.
     * 
     * @param streamsBuilder StreamsBuilder - Kafka Streams 토폴로지를 구성하는 빌더
     */
    public void process(StreamsBuilder streamsBuilder) {
        logger.info("ProductClickProcessor Topology 구성 시작: inputTopic={}, outputTopic={}, windowSize={}, topN={}", 
                productClickTopic, productClickStatsTopic, WINDOW_SIZE, TOP_N);

        // 입력 스트림: product-click 토픽에서 JSON 형식의 상품 클릭 이벤트를 읽음
        // 키: productId (String), 값: ProductClickEventDTO (JSON)
        JsonSerde<ProductClickEventDTO> productClickEventSerde = createJsonSerde(ProductClickEventDTO.class);

        KStream<String, ProductClickEventDTO> clickStream = streamsBuilder.stream(
                productClickTopic,
                Consumed.with(Serdes.String(), productClickEventSerde)
        );

        // 스트림에서 상품 클릭 이벤트 수신 로그 (상품명/회원명은 DB 없이 id만 표시)
        clickStream.peek((key, value) ->
                logger.info("상품 클릭 이벤트 처리: productId={}, memberId={}",
                        key, value != null ? value.memberId() : null)
        );

        // 상품 클릭 통계 스트림 처리
        productClickStatsStream(clickStream);
        // TODO: 이상 클릭(fraud) 탐지를 위한 추가 스트림 처리
        // fraudStream(clickStream);

        logger.info("ProductClickProcessor Topology 구성 완료");
    }

    /**
     * 상품 클릭 통계 스트림 처리
     *
     * JSON ProductClickEventDTO에서 productId를 추출하여, productId를 **키(key)** 로 사용하는 스트림으로 변환한 뒤
     * 15분 간격 타임 윈도우로 상품 클릭 수를 집계하고, 각 윈도우별로 TOP 30 상품을 추출하여
     * State Store에 저장하고 `product-click-stats` 토픽으로 전송합니다.
     *
     * 처리 과정:
     * 1. filter: null 이거나 productId 가 없는 이벤트 제거
     * 2. selectKey: productId 를 Kafka 레코드 키로 설정 (→ 진짜 "상품별" 집계가 됨)
     * 3. groupByKey: 상품 ID(키)를 기준으로 그룹화
     * 4. windowedBy: 15분 간격 타임 윈도우로 집계 (epoch 기준 고정 간격 윈도우)
     * 5. count: 각 윈도우별 상품 클릭 수 집계
     * 6. map: WindowedKey와 count를 ProductClickStatResponse로 변환
     * 7. groupByKey: 윈도우 시작 시간을 키로 그룹화하여 같은 윈도우의 데이터를 묶음
     * 8. aggregate: 각 윈도우별로 TOP 30 상품 추출 및 State Store에 저장
     * 9. to: 결과를 product-click-stats 토픽으로 전송
     *
     * ⚠️ 이 스트림은 State Store에 집계 결과를 유지하고 토픽으로도 전송합니다.
     *    나중에 API 레이어에서 이 상태 스토어를 조회해서 "실시간 상품 클릭 통계"로 활용할 수 있습니다.
     *
     * @param clickStream 상품 클릭 이벤트 스트림 (ProductClickEventDTO)
     */
    private void productClickStatsStream(KStream<String, ProductClickEventDTO> clickStream) {
        // ProductClickEventDTO에서 productId를 추출하여, productId 를 **키(key)** 로 사용하는 스트림으로 변환
        // groupByKey() 가 "상품 ID" 기준으로 동작하도록 selectKey 를 사용한다.
        KStream<String, String> productIdStream = clickStream
                .filter((key, value) -> value != null && value.productId() != null)
                .selectKey((key, event) -> String.valueOf(event.productId()))
                .mapValues(event -> "1"); // 값은 단순 카운트용이므로 의미 없는 상수로 둬도 됨
        // 15분 간격 타임 윈도우로 집계
        // ofSizeWithNoGrace: 윈도우 크기만 지정하고 grace period 없음 (정확한 15분 간격)
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE);

        // 상품 ID를 키로 하여 15분 간격으로 클릭 수 집계
        // 결과: KTable<Windowed<String>, Long>
        // - Windowed<String>: 윈도우 정보와 상품 ID를 포함한 키
        // - Long: 해당 윈도우에서의 클릭 수
        KTable<Windowed<String>, Long> clickCounts = productIdStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(tumblingWindow)
                .count(Materialized.as("product-click-count-store"));  // ⭐ 여기서 RocksDB 생성 & 저장

        // TOP 30 추출 및 출력
        clickCounts
                .toStream()
                // WindowedKey와 count를 ProductClickStatResponse로 변환
                .map((windowedKey, count) -> {
                    String productId = windowedKey.key();
                    Instant windowStart = Instant.ofEpochMilli(windowedKey.window().start());
                    Instant windowEnd = Instant.ofEpochMilli(windowedKey.window().end());
                    
                    logger.debug("상품 클릭 통계: productId={}, count={}, windowStart={}, windowEnd={}", 
                            productId, count, windowStart, windowEnd);
                    
                    // ProductClickStatResponse 생성
                    ProductClickStatResponse stat = new ProductClickStatResponse(
                            productId,
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
                .groupByKey(Grouped.with(Serdes.String(), productClickStatSerde))
                // 각 윈도우별로 TOP 30 상품 추출
                .aggregate(
                        // 초기값: 빈 리스트
                        ArrayList::new,
                        // 집계 함수: 새로운 통계를 리스트에 추가하고 클릭 수 기준으로 정렬하여 TOP 30 유지
                        (key, value, aggregate) -> {
                            aggregate.add(value);
                            // TOP 30 유지 (클릭 수 기준 내림차순)
                            return aggregate.stream()
                                    .sorted(Comparator.comparing(ProductClickStatResponse::clickCount).reversed())
                                    .limit(TOP_N)
                                    .collect(Collectors.toCollection(ArrayList::new));
                        },
                        // State Store 설정: product-click-top30-store에 저장
                        // ⭐ 여기서 RocksDB 생성 & 저장
                        Materialized.<String, List<ProductClickStatResponse>, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("product-click-top30-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(productClickStatListSerde)
                )
                .toStream()
                // 결과를 product-click-stats 토픽으로 전송
                .to(productClickStatsTopic, Produced.with(Serdes.String(), productClickStatListSerde));
    }

    /**
     * 이상 거래 / 비정상 클릭 탐지용 fraudStream (스켈레톤)
     *
     * 이 메서드에는 다음과 같은 로직을 추가할 수 있습니다.
     * - 짧은 시간 안에 특정 productId 에 대한 과도한 클릭 감지
     * - 동일 memberId 의 비정상 클릭 패턴 감지
     * - IP / User-Agent 등 메타데이터가 추가되면 그 기준으로도 이상 패턴 탐지
     *
     * 현재는 구조만 정의해두고, 실제 비즈니스 규칙은 추후 구현을 위해 남겨둡니다.
     */
    @SuppressWarnings("unused")
    private void fraudStream(KStream<String, ProductClickEventDTO> clickStream) {
        // 예시: 기본 골격 (실제 로직은 추후 구현)
        // TimeWindows fraudWindow = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5));
        //
        // clickStream
        //     .groupBy((key, event) -> String.valueOf(event.productId()), Grouped.with(Serdes.String(), productClickEventSerde))
        //     .windowedBy(fraudWindow)
        //     .count()
        //     .toStream()
        //     .filter((windowedKey, count) -> count > SOME_THRESHOLD)
        //     .peek((windowedKey, count) -> logger.warn("잠재적 이상 클릭 감지: productId={}, count={}", windowedKey.key(), count));
    }
}

