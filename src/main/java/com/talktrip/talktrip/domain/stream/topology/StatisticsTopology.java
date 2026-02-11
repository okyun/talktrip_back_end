package com.talktrip.talktrip.domain.stream.topology;

import com.talktrip.talktrip.domain.stream.processor.OrderPurchaseProcessor;
import com.talktrip.talktrip.domain.stream.processor.ProductClickProcessor;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Kafka Streams Topology 설정
 * 
 * 상품 클릭 통계와 주문 구매 통계를 처리하는 Topology를 정의합니다.
 * 15분 간격 윈도우로 집계하여 TOP 30을 추출합니다.
 */
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class StatisticsTopology {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsTopology.class);

    private final StreamsBuilder streamsBuilder;
    private final ProductClickProcessor productClickProcessor;
    private final OrderPurchaseProcessor orderPurchaseProcessor;

    /**
     * Topology 구성
     * StreamsBuilder가 주입된 후 Topology를 구성합니다.
     */
    @PostConstruct
    public void buildTopology() {
        logger.info("Kafka Streams Topology 구성 시작");
        
        // 상품 클릭 통계 처리
        productClickProcessor.process(streamsBuilder);
        
        // 주문 구매 통계 처리
        orderPurchaseProcessor.process(streamsBuilder);
        
        logger.info("Kafka Streams Topology 구성 완료");
    }
}

