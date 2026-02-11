package com.talktrip.talktrip.domain.stream.service;

import com.talktrip.talktrip.domain.messaging.dto.product.ProductClickStatResponse;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 상품 관련 Kafka Streams 서비스
 * 
 * 상품 도메인에 특화된 Kafka Streams 처리 및 조회를 담당합니다.
 * 
 * 주요 기능:
 * 1. 상품 클릭 통계 조회 (State Store)
 * 2. 상품 관련 StreamsMetadata 조회
 * 3. 상품 관련 State Store 조회
 * 
 * State Store:
 * - product-click-top30-store: 상품 클릭 통계 TOP 30
 * - product-click-count-store: 상품 클릭 수 집계
 */
@Service
@RequiredArgsConstructor
public class ProductStreamsService {

    private static final Logger logger = LoggerFactory.getLogger(ProductStreamsService.class);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    // State Store 이름 상수
    private static final String PRODUCT_CLICK_TOP30_STORE = "product-click-top30-store";
    private static final String PRODUCT_CLICK_COUNT_STORE = "product-click-count-store";

    /**
     * KafkaStreams 인스턴스 조회
     * 
     * @return KafkaStreams 인스턴스 (null 가능)
     */
    private KafkaStreams getKafkaStreams() {
        try {
            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
                logger.warn("상품 Streams의 KafkaStreams가 아직 준비되지 않았습니다. state={}",
                        kafkaStreams != null ? kafkaStreams.state() : "null");
                return null;
            }
            return kafkaStreams;
        } catch (Exception e) {
            logger.error("KafkaStreams 인스턴스 조회 실패", e);
            return null;
        }
    }

    /**
     * 상품 클릭 통계 TOP 30 조회
     * 
     * @param windowStartTime 윈도우 시작 시간 (epoch milliseconds, null이면 모든 윈도우)
     * @return 상품 클릭 통계 TOP 30 리스트
     */
    public List<ProductClickStatResponse> getTop30ProductClicks(Long windowStartTime) {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) {
                return List.of();
            }

            ReadOnlyKeyValueStore<String, List<ProductClickStatResponse>> store = 
                    kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                            PRODUCT_CLICK_TOP30_STORE,
                            QueryableStoreTypes.keyValueStore()
                    ));

            String key = windowStartTime != null ? String.valueOf(windowStartTime) : null;
            
            if (key != null) {
                // 특정 윈도우의 TOP 30 조회
                List<ProductClickStatResponse> result = store.get(key);
                return result != null ? result : List.of();
            } else {
                // 모든 윈도우의 TOP 30 조회 (최신 윈도우 우선)
                List<ProductClickStatResponse> allResults = new ArrayList<>();
                try (KeyValueIterator<String, List<ProductClickStatResponse>> iterator = store.all()) {
                    while (iterator.hasNext()) {
                        KeyValue<String, List<ProductClickStatResponse>> entry = iterator.next();
                        allResults.addAll(entry.value);
                    }
                }
                
                // 클릭 수 기준으로 정렬하여 TOP 30 반환
                return allResults.stream()
                        .sorted(Comparator.comparing(ProductClickStatResponse::clickCount).reversed())
                        .limit(30)
                        .collect(Collectors.toList());
            }
        } catch (Exception e) {
            logger.error("상품 클릭 통계 조회 실패", e);
            return List.of();
        }
    }

    /**
     * 상품 관련 StreamsMetadata 조회
     * 
     * @return StreamsMetadata 리스트
     */
    @SuppressWarnings("deprecation")
    public List<StreamsMetadata> getProductStreamsMetadata() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) {
                return List.of();
            }
            
            Collection<StreamsMetadata> allMetadata = kafkaStreams.allMetadata();
            // 상품 관련 Store를 포함하는 메타데이터만 필터링
            return allMetadata.stream()
                    .filter(metadata -> metadata.stateStoreNames().contains(PRODUCT_CLICK_TOP30_STORE) ||
                                      metadata.stateStoreNames().contains(PRODUCT_CLICK_COUNT_STORE))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("상품 StreamsMetadata 조회 실패", e);
            return List.of();
        }
    }

    /**
     * 상품 클릭 통계 Store 조회
     * 
     * @return ReadOnlyKeyValueStore (null 가능)
     */
    public ReadOnlyKeyValueStore<String, List<ProductClickStatResponse>> getProductClickTop30Store() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) {
                return null;
            }
            
            return kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                    PRODUCT_CLICK_TOP30_STORE,
                    QueryableStoreTypes.keyValueStore()
            ));
        } catch (Exception e) {
            logger.error("상품 클릭 통계 Store 조회 실패", e);
            return null;
        }
    }

    /**
     * 상품 Streams 상태 확인
     * 
     * @return 준비되었으면 true, 아니면 false
     */
    public boolean isReady() {
        KafkaStreams kafkaStreams = getKafkaStreams();
        return kafkaStreams != null;
    }
}

