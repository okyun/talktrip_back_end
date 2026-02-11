package com.talktrip.talktrip.domain.stream.processor;

import com.talktrip.talktrip.domain.messaging.dto.product.ProductClickStatResponse;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
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
 * 상품 클릭 이벤트를 15분 간격으로 집계하여 TOP 30을 추출합니다.
 * 0시부터 시작하여 15분 간격으로 윈도우를 생성합니다.
 * 
 * 애플리케이션이 시작될 때(Spring이 빈 만들 때) 한 번만 실행되고,
 * 그 결과로 만들어진 토폴로지를 기반으로
 * KafkaStreams가 계속 메시지를 감시/처리하는 구조입니다.
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

    // Avro Serde - KafkaStreamConfig에서 Bean으로 주입받음
    private final GenericAvroSerde genericAvroSerde;

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
     * 애플리케이션 시작 시 한 번만 실행되며, 다음과 같은 처리를 수행합니다:
     * 1. product-click 토픽에서 Avro 형식의 상품 클릭 이벤트를 스트림으로 읽어옵니다.
     * 2. Avro GenericRecord에서 productId를 추출합니다.
     * 3. 15분 간격 타임 윈도우로 클릭 수를 집계합니다.
     * 4. 각 윈도우별로 클릭 수가 많은 상위 30개 상품을 추출합니다.
     * 5. 결과를 product-click-stats 토픽으로 전송하고 State Store에 저장합니다.
     * 
     * State Store는 나중에 API 레이어에서 조회하여 "실시간 상품 클릭 통계"로 활용할 수 있습니다.
     * 
     * @param streamsBuilder StreamsBuilder - Kafka Streams 토폴로지를 구성하는 빌더
     */
    public void process(StreamsBuilder streamsBuilder) {
        logger.info("ProductClickProcessor Topology 구성 시작: inputTopic={}, outputTopic={}, windowSize={}, topN={}", 
                productClickTopic, productClickStatsTopic, WINDOW_SIZE, TOP_N);

        // 입력 스트림: product-click 토픽에서 Avro 형식의 상품 클릭 이벤트를 읽음
        // 키: productId (String), 값: Avro GenericRecord (ProductClickEvent)
        // genericAvroSerde는 KafkaStreamConfig에서 Bean으로 주입받음
        KStream<String, GenericRecord> clickStream = streamsBuilder.stream(
                productClickTopic,
                Consumed.with(Serdes.String(), genericAvroSerde)
        );

        // 상품 클릭 통계 스트림 처리
        productClickStatsStream(clickStream);

        logger.info("ProductClickProcessor Topology 구성 완료");
    }

    /**
     * 상품 클릭 통계 스트림 처리
     * 
     * Avro GenericRecord에서 productId를 추출하여 15분 간격 타임 윈도우로 상품 클릭 수를 집계하고,
     * 각 윈도우별로 TOP 30 상품을 추출하여 State Store에 저장하고 토픽으로 전송합니다.
     * 
     * 처리 과정:
     * 1. mapValues: Avro GenericRecord에서 productId를 추출하여 String으로 변환
     * 2. groupByKey: 상품 ID를 키로 그룹화
     * 3. windowedBy: 15분 간격 타임 윈도우로 집계
     * 4. count: 각 윈도우별 상품 클릭 수 집계
     * 5. map: WindowedKey와 count를 ProductClickStatResponse로 변환
     * 6. groupByKey: 윈도우 시작 시간을 키로 그룹화하여 같은 윈도우의 데이터를 묶음
     * 7. aggregate: 각 윈도우별로 TOP 30 상품 추출 및 State Store에 저장
     * 8. to: 결과를 product-click-stats 토픽으로 전송
     * 
     * ⚠️ 이 스트림은 State Store에 집계 결과를 유지하고 토픽으로도 전송합니다.
     * → 나중에 API 레이어에서 이 상태 스토어를 조회해서 "실시간 상품 클릭 통계"로 활용 가능
     * 
     * @param clickStream 상품 클릭 이벤트 스트림 (Avro GenericRecord)
     */
    private void productClickStatsStream(KStream<String, GenericRecord> clickStream) {
        // Avro GenericRecord에서 productId를 추출하여 String 스트림으로 변환
        KStream<String, String> productIdStream = clickStream.mapValues(record -> {
            try {
                // Avro 스키마에 따라 productId는 long 타입
                Long productId = (Long) record.get("productId");
                return String.valueOf(productId);
            } catch (Exception e) {
                logger.error("Avro 레코드에서 productId 추출 실패: record={}", record, e);
                return null; // null은 필터링됨
            }
        }).filter((key, value) -> value != null); // null 값 필터링
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
}

