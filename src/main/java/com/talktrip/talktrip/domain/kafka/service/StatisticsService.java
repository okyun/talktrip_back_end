package com.talktrip.talktrip.domain.kafka.service;

import com.talktrip.talktrip.domain.messaging.dto.order.OrderPurchaseStatResponse;
import com.talktrip.talktrip.domain.messaging.dto.product.ProductClickStatResponse;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 통계 조회 서비스
 * 
 * Kafka Streams State Store에서 통계 데이터를 조회합니다.
 */
@Service
@RequiredArgsConstructor
public class StatisticsService {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsService.class);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    /**
     * 상품 클릭 통계 TOP 30 조회
     * 
     * @param windowStartTime 윈도우 시작 시간 (epoch milliseconds)
     * @return 상품 클릭 통계 TOP 30 리스트
     */
    public List<ProductClickStatResponse> getTop30ProductClicks(Long windowStartTime) {
        try {
            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
                logger.warn("상품 클릭의 KafkaStreams가 아직 준비되지 않았습니다. state={}",
                        kafkaStreams != null ? kafkaStreams.state() : "null");
                return List.of();
            }

            ReadOnlyKeyValueStore<String, List<ProductClickStatResponse>> store = 
                    kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                            "product-click-top30-store",
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
     * 주문 구매 통계 TOP 30 조회
     * 
     * @param windowStartTime 윈도우 시작 시간 (epoch milliseconds)
     * @return 주문 구매 통계 TOP 30 리스트
     */
    public List<OrderPurchaseStatResponse> getTop30OrderPurchases(Long windowStartTime) {
        try {
            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
                logger.warn("구매에 KafkaStreams가 아직 준비되지 않았습니다. state={}",
                        kafkaStreams != null ? kafkaStreams.state() : "null");
                return List.of();
            }

            ReadOnlyKeyValueStore<String, List<OrderPurchaseStatResponse>> store = 
                    kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                            "order-purchase-top30-store",
                            QueryableStoreTypes.keyValueStore()
                    ));

            String key = windowStartTime != null ? String.valueOf(windowStartTime) : null;
            
            if (key != null) {
                // 특정 윈도우의 TOP 30 조회
                List<OrderPurchaseStatResponse> result = store.get(key);
                return result != null ? result : List.of();
            } else {
                // 모든 윈도우의 TOP 30 조회 (최신 윈도우 우선)
                List<OrderPurchaseStatResponse> allResults = new ArrayList<>();
                try (KeyValueIterator<String, List<OrderPurchaseStatResponse>> iterator = store.all()) {
                    while (iterator.hasNext()) {
                        KeyValue<String, List<OrderPurchaseStatResponse>> entry = iterator.next();
                        allResults.addAll(entry.value);
                    }
                }
                
                // 구매 수 기준으로 정렬하여 TOP 30 반환
                return allResults.stream()
                        .sorted(Comparator.comparing(OrderPurchaseStatResponse::purchaseCount).reversed())
                        .limit(30)
                        .collect(Collectors.toList());
            }
        } catch (Exception e) {
            logger.error("주문 구매 통계 조회 실패", e);
            return List.of();
        }
    }
}

