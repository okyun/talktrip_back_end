package com.talktrip.talktrip.domain.messaging.avro;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Avro 스키마 관리자
 * 
 * resources/avro/ 폴더에 있는 .avsc 파일에서 Avro 스키마를 로드하고 캐싱하여 재사용합니다.
 * Schema Registry에 등록하기 전에 스키마를 검증하고 관리합니다.
 */
@Component
public class SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    // 스키마 파일 경로
    private static final String SCHEMA_BASE_PATH = "avro/";
    
    // 스키마 캐시 (스키마 이름 -> Schema 객체)
    private final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();

    /**
     * 주문 생성 이벤트 스키마 생성
     * 
     * resources/avro/OrderCreatedEvent.avsc 파일에서 스키마를 로드합니다.
     * 
     * @return OrderCreatedEvent Avro 스키마
     */
    public Schema getOrderCreatedEventSchema() {
        return schemaCache.computeIfAbsent("OrderCreatedEvent", key -> {
            String schemaString = loadSchemaFromFile("OrderCreatedEvent.avsc");
            return parseSchema(schemaString);
        });
    }

    /**
     * 상품 클릭 이벤트 스키마 생성
     * 
     * resources/avro/ProductClickEvent.avsc 파일에서 스키마를 로드합니다.
     * 
     * @return ProductClickEvent Avro 스키마
     */
    public Schema getProductClickEventSchema() {
        return schemaCache.computeIfAbsent("ProductClickEvent", key -> {
            String schemaString = loadSchemaFromFile("ProductClickEvent.avsc");
            return parseSchema(schemaString);
        });
    }

    /**
     * 주문 구매 통계 스키마 생성
     * 
     * resources/avro/OrderPurchaseStats.avsc 파일에서 스키마를 로드합니다.
     * 
     * @return OrderPurchaseStats Avro 스키마
     */
    public Schema getOrderPurchaseStatsSchema() {
        return schemaCache.computeIfAbsent("OrderPurchaseStats", key -> {
            String schemaString = loadSchemaFromFile("OrderPurchaseStats.avsc");
            return parseSchema(schemaString);
        });
    }

    /**
     * resources/avro/ 폴더에서 스키마 파일을 로드합니다.
     * 
     * @param fileName 스키마 파일 이름 (예: "OrderCreatedEvent.avsc")
     * @return 스키마 파일 내용 (JSON 문자열)
     */
    private String loadSchemaFromFile(String fileName) {
        try {
            ClassPathResource resource = new ClassPathResource(SCHEMA_BASE_PATH + fileName);
            String schemaString = StreamUtils.copyToString(
                    resource.getInputStream(), 
                    StandardCharsets.UTF_8
            );
            logger.debug("Avro 스키마 파일 로드 성공: {}", fileName);
            return schemaString;
        } catch (IOException e) {
            logger.error("Avro 스키마 파일 로드 실패: {}", fileName, e);
            throw new RuntimeException("Avro 스키마 파일 로드 실패: " + fileName, e);
        }
    }

    /**
     * 스키마 문자열을 파싱하여 Schema 객체로 변환
     * 
     * @param schemaString JSON 형식의 스키마 문자열
     * @return 파싱된 Schema 객체
     */
    private Schema parseSchema(String schemaString) {
        try {
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(schemaString);
            logger.debug("Avro 스키마 파싱 성공: {}", schema.getFullName());
            return schema;
        } catch (Exception e) {
            logger.error("Avro 스키마 파싱 실패: {}", schemaString, e);
            throw new RuntimeException("Avro 스키마 파싱 실패", e);
        }
    }

    /**
     * 스키마 캐시 초기화
     * 
     * 필요시 모든 스키마를 다시 로드합니다.
     */
    public void clearCache() {
        schemaCache.clear();
        logger.info("Avro 스키마 캐시 초기화 완료");
    }

    /**
     * 특정 스키마를 캐시에서 제거
     * 
     * @param schemaName 스키마 이름
     */
    public void removeSchema(String schemaName) {
        schemaCache.remove(schemaName);
        logger.debug("Avro 스키마 캐시에서 제거: {}", schemaName);
    }
}

