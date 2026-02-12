package com.talktrip.talktrip.domain.stream.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Kafka Streams 서비스
 * 
 * Kafka Streams 애플리케이션의 생명주기 관리 및 모니터링을 담당합니다.
 * 
 * 주요 기능:
 * 1. KafkaStreams 상태 조회 및 관리
 * 2. Topology 정보 조회
 * 3. Streams 애플리케이션 시작/중지/재시작
 * 4. StreamsMetadata 조회 (클러스터 정보)
 * 5. State Store 목록 조회
 * 
 * 참고:
 * - StatisticsService는 특정 통계 데이터 조회에 집중
 * - StreamsService는 Streams 애플리케이션 전반의 관리에 집중
 */
@Service
@RequiredArgsConstructor
public class StreamsService {

    private static final Logger logger = LoggerFactory.getLogger(StreamsService.class);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;// 카프카 스트림 컨테이너를 관리해서 시간 기반 데이터를 조회 하는데 사용된다.

    /**
     * KafkaStreams 인스턴스 조회
     * 
     * @return KafkaStreams 인스턴스 (null 가능)
     */
    public KafkaStreams getKafkaStreams() {
        try {
            return streamsBuilderFactoryBean.getKafkaStreams();
        } catch (Exception e) {
            logger.error("KafkaStreams 인스턴스 조회 실패", e);
            return null;
        }
    }

    /**
     * KafkaStreams 상태 조회
     * 
     * @return KafkaStreams.State (null 가능)
     */
    public KafkaStreams.State getState() {
        KafkaStreams kafkaStreams = getKafkaStreams();
        if (kafkaStreams == null) {
            return null;
        }
        return kafkaStreams.state();
    }

    /**
     * KafkaStreams가 실행 중인지 확인
     * 
     * @return 실행 중이면 true, 아니면 false
     */
    public boolean isRunning() {
        KafkaStreams.State state = getState();
        return state == KafkaStreams.State.RUNNING;
    }

    /**
     * KafkaStreams가 준비되었는지 확인
     * 
     * @return 준비되었으면 true, 아니면 false
     */
    public boolean isReady() {
        KafkaStreams kafkaStreams = getKafkaStreams();
        if (kafkaStreams == null) {
            return false;
        }
        KafkaStreams.State state = kafkaStreams.state();
        return state == KafkaStreams.State.RUNNING || state == KafkaStreams.State.REBALANCING;
    }

    /**
     * Topology 정보 조회
     * 
     * StreamsBuilderFactoryBean에서 Topology를 가져와 설명을 반환합니다.
     * 
     * @return TopologyDescription 문자열 (null 가능)
     */
    public String getTopologyDescription() {
        try {
            // StreamsBuilderFactoryBean에서 Topology를 직접 가져올 수 없으므로
            // KafkaStreams를 통해 간접적으로 확인
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) {
                logger.warn("KafkaStreams 인스턴스가 없어 Topology 정보를 조회할 수 없습니다.");
                return null;
            }
            // Topology 정보는 StreamsBuilderFactoryBean에서 직접 가져와야 함
            // 현재는 상태 정보만 반환
            return "Topology 정보는 StreamsBuilderFactoryBean에서 직접 조회해야 합니다.";
        } catch (Exception e) {
            logger.error("Topology 정보 조회 실패", e);
            return null;
        }
    }

    /**
     * StreamsMetadata 목록 조회
     * 
     * 모든 스트림 인스턴스의 메타데이터를 조회합니다.
     * 
     * @return StreamsMetadata 리스트
     */
    @SuppressWarnings("deprecation")
    public List<StreamsMetadata> getAllStreamsMetadata() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) {
                logger.warn("KafkaStreams 인스턴스가 없어 StreamsMetadata를 조회할 수 없습니다.");
                return List.of();
            }
            Collection<StreamsMetadata> metadataCollection = kafkaStreams.allMetadata();
            return new ArrayList<>(metadataCollection);
        } catch (Exception e) {
            logger.error("StreamsMetadata 조회 실패", e);
            return List.of();
        }
    }

    /**
     * 특정 Store의 StreamsMetadata 조회
     * 
     * @param storeName State Store 이름
     * @param key 조회할 키
     * @return StreamsMetadata (null 가능)
     */
    @SuppressWarnings("deprecation")
    public StreamsMetadata getStreamsMetadataForStore(String storeName, String key) {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) {
                logger.warn("KafkaStreams 인스턴스가 없어 StreamsMetadata를 조회할 수 없습니다.");
                return null;
            }
            // allMetadata()를 사용하여 필터링
            Collection<StreamsMetadata> allMetadata = kafkaStreams.allMetadata();
            return allMetadata.stream()
                    .filter(metadata -> metadata.stateStoreNames().contains(storeName))
                    .findFirst()
                    .orElse(null);
        } catch (Exception e) {
            logger.error("StreamsMetadata 조회 실패: storeName={}, key={}", storeName, key, e);
            return null;
        }
    }

    /**
     * KafkaStreams 시작
     * 
     * @return 성공하면 true, 실패하면 false
     */
    public boolean start() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) {
                logger.warn("KafkaStreams 인스턴스가 없어 시작할 수 없습니다.");
                return false;
            }
            
            KafkaStreams.State currentState = kafkaStreams.state();
            if (currentState == KafkaStreams.State.RUNNING) {
                logger.info("KafkaStreams가 이미 실행 중입니다. state={}", currentState);
                return true;
            }
            
            kafkaStreams.start();
            logger.info("KafkaStreams 시작 완료. 이전 state={}, 현재 state={}", 
                    currentState, kafkaStreams.state());
            return true;
        } catch (Exception e) {
            logger.error("KafkaStreams 시작 실패", e);
            return false;
        }
    }

    /**
     * KafkaStreams 중지
     * 
     * @return 성공하면 true, 실패하면 false
     */
    public boolean stop() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) {
                logger.warn("KafkaStreams 인스턴스가 없어 중지할 수 없습니다.");
                return false;
            }
            
            KafkaStreams.State currentState = kafkaStreams.state();
            if (currentState == KafkaStreams.State.NOT_RUNNING) {
                logger.info("KafkaStreams가 이미 중지되어 있습니다. state={}", currentState);
                return true;
            }
            
            kafkaStreams.close();
            logger.info("KafkaStreams 중지 완료. 이전 state={}", currentState);
            return true;
        } catch (Exception e) {
            logger.error("KafkaStreams 중지 실패", e);
            return false;
        }
    }

    /**
     * KafkaStreams 재시작
     * 
     * @return 성공하면 true, 실패하면 false
     */
    public boolean restart() {
        try {
            logger.info("KafkaStreams 재시작 시작");
            boolean stopped = stop();
            if (!stopped) {
                logger.warn("KafkaStreams 중지 실패로 인해 재시작을 건너뜁니다.");
                return false;
            }
            
            // 잠시 대기 (상태 전환 시간 확보)
            Thread.sleep(1000);
            
            boolean started = start();
            if (!started) {
                logger.error("KafkaStreams 시작 실패로 인해 재시작이 완료되지 않았습니다.");
                return false;
            }
            
            logger.info("KafkaStreams 재시작 완료");
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("KafkaStreams 재시작 중 인터럽트 발생", e);
            return false;
        } catch (Exception e) {
            logger.error("KafkaStreams 재시작 실패", e);
            return false;
        }
    }

    /**
     * Streams 애플리케이션 상태 정보 조회
     * 
     * @return 상태 정보 문자열
     */
    public String getStatusInfo() {
        KafkaStreams kafkaStreams = getKafkaStreams();
        if (kafkaStreams == null) {
            return "KafkaStreams 인스턴스가 없습니다.";
        }
        
        KafkaStreams.State state = kafkaStreams.state();
        String topology = getTopologyDescription();
        List<StreamsMetadata> metadata = getAllStreamsMetadata();
        
        StringBuilder info = new StringBuilder();
        info.append("KafkaStreams 상태 정보:\n");
        info.append("  - State: ").append(state).append("\n");
        info.append("  - Running: ").append(isRunning()).append("\n");
        info.append("  - Ready: ").append(isReady()).append("\n");
        
        if (topology != null) {
            info.append("  - Topology: ").append(topology).append("\n");
        }
        
        info.append("  - StreamsMetadata 개수: ").append(metadata.size()).append("\n");
        
        return info.toString();
    }
}

