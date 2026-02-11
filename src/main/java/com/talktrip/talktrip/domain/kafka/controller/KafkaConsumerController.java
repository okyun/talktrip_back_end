package com.talktrip.talktrip.domain.kafka.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Kafka Consumer 컨트롤러
 * 
 * Kafka Consumer의 상태를 조회하고 제어하는 엔드포인트를 제공합니다.
 * Consumer가 정상적으로 동작하는지 확인하고, 필요시 시작/중지할 수 있습니다.
 */
@Tag(name = "Kafka Consumer", description = "Kafka Consumer 상태 조회 및 제어 API")
@RestController
@RequestMapping("/api/kafka/consumer")
@RequiredArgsConstructor
public class KafkaConsumerController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerController.class);

    private final ApplicationContext applicationContext;

    /**
     * KafkaListenerEndpointRegistry를 ApplicationContext에서 가져옵니다.
     * 
     * @return KafkaListenerEndpointRegistry
     */
    private KafkaListenerEndpointRegistry getKafkaListenerEndpointRegistry() {
        try {
            return applicationContext.getBean(KafkaListenerEndpointRegistry.class);
        } catch (Exception e) {
            logger.warn("KafkaListenerEndpointRegistry를 찾을 수 없습니다. Kafka가 제대로 설정되지 않았을 수 있습니다.", e);
            return null;
        }
    }

    /**
     * 모든 Consumer 상태 조회
     * 
     * 등록된 모든 Kafka Consumer의 상태를 조회합니다.
     * 
     * @return Consumer 상태 정보
     */
    @Operation(
            summary = "모든 Consumer 상태 조회",
            description = "등록된 모든 Kafka Consumer의 상태를 조회합니다."
    )
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getAllConsumerStatus() {
        Map<String, Object> response = new HashMap<>();
        
        try {
            KafkaListenerEndpointRegistry registry = getKafkaListenerEndpointRegistry();
            if (registry == null) {
                response.put("success", false);
                response.put("message", "KafkaListenerEndpointRegistry를 사용할 수 없습니다.");
                return ResponseEntity.internalServerError().body(response);
            }
            
            Map<String, Map<String, Object>> consumers = registry
                    .getListenerContainers()
                    .stream()
                    .collect(Collectors.toMap(
                            MessageListenerContainer::getListenerId,
                            container -> {
                                Map<String, Object> status = new HashMap<>();
                                status.put("listenerId", container.getListenerId());
                                status.put("isRunning", container.isRunning());
                                status.put("isContainerPaused", container.isContainerPaused());
                                status.put("isPauseRequested", container.isPauseRequested());
                                status.put("isRunning", container.isRunning());
                                
                                // Consumer 정보
                                if (container.getContainerProperties() != null) {
                                    status.put("groupId", container.getContainerProperties().getGroupId());
                                    status.put("topics", container.getContainerProperties().getTopics());
                                }
                                
                                return status;
                            }
                    ));
            
            response.put("success", true);
            response.put("consumers", consumers);
            response.put("totalCount", consumers.size());
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Consumer 상태 조회 실패", e);
            response.put("success", false);
            response.put("message", "Consumer 상태 조회 실패: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * 특정 Consumer 상태 조회
     * 
     * @param listenerId Consumer 리스너 ID
     * @return Consumer 상태 정보
     */
    @Operation(
            summary = "특정 Consumer 상태 조회",
            description = "지정된 Consumer의 상태를 조회합니다."
    )
    @GetMapping("/status/{listenerId}")
    public ResponseEntity<Map<String, Object>> getConsumerStatus(@PathVariable String listenerId) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            KafkaListenerEndpointRegistry registry = getKafkaListenerEndpointRegistry();
            if (registry == null) {
                response.put("success", false);
                response.put("message", "KafkaListenerEndpointRegistry를 사용할 수 없습니다.");
                return ResponseEntity.internalServerError().body(response);
            }
            
            MessageListenerContainer container = registry.getListenerContainer(listenerId);
            
            if (container == null) {
                response.put("success", false);
                response.put("message", "Consumer를 찾을 수 없습니다: " + listenerId);
                return ResponseEntity.notFound().build();
            }
            
            Map<String, Object> status = new HashMap<>();
            status.put("listenerId", container.getListenerId());
            status.put("isRunning", container.isRunning());
            status.put("isContainerPaused", container.isContainerPaused());
            status.put("isPauseRequested", container.isPauseRequested());
            
            if (container.getContainerProperties() != null) {
                status.put("groupId", container.getContainerProperties().getGroupId());
                status.put("topics", container.getContainerProperties().getTopics());
            }
            
            response.put("success", true);
            response.put("consumer", status);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Consumer 상태 조회 실패: listenerId={}", listenerId, e);
            response.put("success", false);
            response.put("message", "Consumer 상태 조회 실패: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * Consumer 시작
     * 
     * 중지된 Consumer를 시작합니다.
     * 
     * @param listenerId Consumer 리스너 ID
     * @return 작업 결과
     */
    @Operation(
            summary = "Consumer 시작",
            description = "중지된 Consumer를 시작합니다."
    )
    @PostMapping("/start/{listenerId}")
    public ResponseEntity<Map<String, Object>> startConsumer(@PathVariable String listenerId) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            KafkaListenerEndpointRegistry registry = getKafkaListenerEndpointRegistry();
            if (registry == null) {
                response.put("success", false);
                response.put("message", "KafkaListenerEndpointRegistry를 사용할 수 없습니다.");
                return ResponseEntity.internalServerError().body(response);
            }
            
            MessageListenerContainer container = registry.getListenerContainer(listenerId);
            
            if (container == null) {
                response.put("success", false);
                response.put("message", "Consumer를 찾을 수 없습니다: " + listenerId);
                return ResponseEntity.notFound().build();
            }
            
            if (container.isRunning()) {
                response.put("success", false);
                response.put("message", "Consumer가 이미 실행 중입니다: " + listenerId);
                return ResponseEntity.badRequest().body(response);
            }
            
            container.start();
            logger.info("Consumer 시작: listenerId={}", listenerId);
            
            response.put("success", true);
            response.put("message", "Consumer가 시작되었습니다: " + listenerId);
            response.put("listenerId", listenerId);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Consumer 시작 실패: listenerId={}", listenerId, e);
            response.put("success", false);
            response.put("message", "Consumer 시작 실패: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * Consumer 중지
     * 
     * 실행 중인 Consumer를 중지합니다.
     * 
     * @param listenerId Consumer 리스너 ID
     * @return 작업 결과
     */
    @Operation(
            summary = "Consumer 중지",
            description = "실행 중인 Consumer를 중지합니다."
    )
    @PostMapping("/stop/{listenerId}")
    public ResponseEntity<Map<String, Object>> stopConsumer(@PathVariable String listenerId) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            KafkaListenerEndpointRegistry registry = getKafkaListenerEndpointRegistry();
            if (registry == null) {
                response.put("success", false);
                response.put("message", "KafkaListenerEndpointRegistry를 사용할 수 없습니다.");
                return ResponseEntity.internalServerError().body(response);
            }
            
            MessageListenerContainer container = registry.getListenerContainer(listenerId);
            
            if (container == null) {
                response.put("success", false);
                response.put("message", "Consumer를 찾을 수 없습니다: " + listenerId);
                return ResponseEntity.notFound().build();
            }
            
            if (!container.isRunning()) {
                response.put("success", false);
                response.put("message", "Consumer가 이미 중지되어 있습니다: " + listenerId);
                return ResponseEntity.badRequest().body(response);
            }
            
            container.stop();
            logger.info("Consumer 중지: listenerId={}", listenerId);
            
            response.put("success", true);
            response.put("message", "Consumer가 중지되었습니다: " + listenerId);
            response.put("listenerId", listenerId);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Consumer 중지 실패: listenerId={}", listenerId, e);
            response.put("success", false);
            response.put("message", "Consumer 중지 실패: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * Consumer 일시 중지
     * 
     * 실행 중인 Consumer를 일시 중지합니다.
     * 
     * @param listenerId Consumer 리스너 ID
     * @return 작업 결과
     */
    @Operation(
            summary = "Consumer 일시 중지",
            description = "실행 중인 Consumer를 일시 중지합니다."
    )
    @PostMapping("/pause/{listenerId}")
    public ResponseEntity<Map<String, Object>> pauseConsumer(@PathVariable String listenerId) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            KafkaListenerEndpointRegistry registry = getKafkaListenerEndpointRegistry();
            if (registry == null) {
                response.put("success", false);
                response.put("message", "KafkaListenerEndpointRegistry를 사용할 수 없습니다.");
                return ResponseEntity.internalServerError().body(response);
            }
            
            MessageListenerContainer container = registry.getListenerContainer(listenerId);
            
            if (container == null) {
                response.put("success", false);
                response.put("message", "Consumer를 찾을 수 없습니다: " + listenerId);
                return ResponseEntity.notFound().build();
            }
            
            if (!container.isRunning()) {
                response.put("success", false);
                response.put("message", "Consumer가 실행 중이 아닙니다: " + listenerId);
                return ResponseEntity.badRequest().body(response);
            }
            
            container.pause();
            logger.info("Consumer 일시 중지: listenerId={}", listenerId);
            
            response.put("success", true);
            response.put("message", "Consumer가 일시 중지되었습니다: " + listenerId);
            response.put("listenerId", listenerId);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Consumer 일시 중지 실패: listenerId={}", listenerId, e);
            response.put("success", false);
            response.put("message", "Consumer 일시 중지 실패: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    /**
     * Consumer 재개
     * 
     * 일시 중지된 Consumer를 재개합니다.
     * 
     * @param listenerId Consumer 리스너 ID
     * @return 작업 결과
     */
    @Operation(
            summary = "Consumer 재개",
            description = "일시 중지된 Consumer를 재개합니다."
    )
    @PostMapping("/resume/{listenerId}")
    public ResponseEntity<Map<String, Object>> resumeConsumer(@PathVariable String listenerId) {
        Map<String, Object> response = new HashMap<>();
        
        try {
            KafkaListenerEndpointRegistry registry = getKafkaListenerEndpointRegistry();
            if (registry == null) {
                response.put("success", false);
                response.put("message", "KafkaListenerEndpointRegistry를 사용할 수 없습니다.");
                return ResponseEntity.internalServerError().body(response);
            }
            
            MessageListenerContainer container = registry.getListenerContainer(listenerId);
            
            if (container == null) {
                response.put("success", false);
                response.put("message", "Consumer를 찾을 수 없습니다: " + listenerId);
                return ResponseEntity.notFound().build();
            }
            
            if (!container.isContainerPaused()) {
                response.put("success", false);
                response.put("message", "Consumer가 일시 중지되어 있지 않습니다: " + listenerId);
                return ResponseEntity.badRequest().body(response);
            }
            
            container.resume();
            logger.info("Consumer 재개: listenerId={}", listenerId);
            
            response.put("success", true);
            response.put("message", "Consumer가 재개되었습니다: " + listenerId);
            response.put("listenerId", listenerId);
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Consumer 재개 실패: listenerId={}", listenerId, e);
            response.put("success", false);
            response.put("message", "Consumer 재개 실패: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
}

