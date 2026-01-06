package com.talktrip.talktrip.global.config;

import com.talktrip.talktrip.domain.messaging.dto.order.OrderEvent;
import com.talktrip.talktrip.domain.messaging.dto.product.ProductEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

/**
 * Kafka Consumer 및 Producer 설정
 * 
 * 다양한 메시지 타입(JSON, Avro, String)을 처리하기 위한 Factory들을 제공합니다.
 * ErrorHandlingDeserializer를 사용하여 역직렬화 실패 시 애플리케이션 종료를 방지합니다.
 * 
 * @EnableKafka: Kafka Listener를 활성화하고 KafkaListenerEndpointRegistry를 자동 등록합니다.
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    // ----------- Consumer Factory (Listener) --------------

    /**
     * OrderEvent 전용 Consumer Factory
     * 
     * ErrorHandlingDeserializer를 사용하여 역직렬화 실패 시 애플리케이션 종료를 방지합니다.
     * JsonDeserializer를 사용하여 OrderEvent 타입으로 자동 역직렬화합니다.
     * 메시지 포맷이 잘못되어도 consumer가 중단되지 않습니다.
     */
    @Bean
    public ConsumerFactory<String, OrderEvent> orderEventConsumerFactory() {
        // ErrorHandlingDeserializer - 역직렬화 실패 시 애플리케이션 종료 방지, 잘못된 JSON 방식 처리
        Map<String, Object> props = Map.ofEntries(
                Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class),
                Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class),
                Map.entry(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class),
                Map.entry(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class),
                // 메시지 포맷이 잘못되어도 consumer 중단하지 않는 형태
                Map.entry(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class),
                Map.entry(JsonDeserializer.TRUSTED_PACKAGES, "*"), // 전체 path 허용
                Map.entry(JsonDeserializer.USE_TYPE_INFO_HEADERS, false)
        );
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * OrderEvent 전용 Kafka Listener Container Factory
     * 
     * @KafkaListener 어노테이션의 수신을 위한 Bean
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> orderEventKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(orderEventConsumerFactory());
        return factory;
    }

    /**
     * ProductEvent 전용 Consumer Factory
     * 
     * ErrorHandlingDeserializer를 사용하여 역직렬화 실패 시 애플리케이션 종료를 방지합니다.
     * JsonDeserializer를 사용하여 ProductEvent 타입으로 자동 역직렬화합니다.
     * 메시지 포맷이 잘못되어도 consumer가 중단되지 않습니다.
     */
    @Bean
    public ConsumerFactory<String, ProductEvent> productEventConsumerFactory() {
        // ErrorHandlingDeserializer - 역직렬화 실패 시 애플리케이션 종료 방지, 잘못된 JSON 방식 처리
        Map<String, Object> props = Map.ofEntries(
                Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class),
                Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class),
                Map.entry(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class),
                Map.entry(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class),
                // 메시지 포맷이 잘못되어도 consumer 중단하지 않는 형태
                Map.entry(JsonDeserializer.VALUE_DEFAULT_TYPE, ProductEvent.class),
                Map.entry(JsonDeserializer.TRUSTED_PACKAGES, "*"), // 전체 path 허용
                Map.entry(JsonDeserializer.USE_TYPE_INFO_HEADERS, false)
        );
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * ProductEvent 전용 Kafka Listener Container Factory
     * 
     * @KafkaListener 어노테이션의 수신을 위한 Bean
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ProductEvent> productEventKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ProductEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(productEventConsumerFactory());
        return factory;
    }

    /**
     * Generic Consumer Factory
     * 
     * OrderEvent가 아닌 다른 타입도 메시지로 받기 위한 메소드
     * USE_TYPE_INFO_HEADERS를 true로 설정하여 타입 정보를 헤더에서 읽어옵니다.
     */
    @Bean
    public ConsumerFactory<String, Object> genericConsumerFactory() {
        Map<String, Object> props = Map.ofEntries(
                Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class),
                Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class),
                Map.entry(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class),
                Map.entry(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class),
                Map.entry(JsonDeserializer.TRUSTED_PACKAGES, "*"),
                Map.entry(JsonDeserializer.USE_TYPE_INFO_HEADERS, true)
        );
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Generic Kafka Listener Container Factory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> genericKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(genericConsumerFactory());
        return factory;
    }

    /**
     * Avro Consumer Factory
     * 
     * Apache Avro = JSON처럼 생겼지만 훨씬 빠르고, 가볍고,
     * Schema(스키마)를 함께 사용해서 안전하게 직렬화/역직렬화하는 데이터 포맷.
     * GenericRecord를 반환하여 유연하게 처리합니다.
     */
    @Bean
    public ConsumerFactory<String, GenericRecord> avroConsumerFactory() {
        Map<String, Object> props = Map.ofEntries(
                Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
                Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class),
                Map.entry(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl),
                Map.entry(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false),
                Map.entry(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true)
        );
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Avro Kafka Listener Container Factory
     * 
     * 외부 시스템에서 Avro 형식의 메시지를 수신할 때 사용합니다.
     * 내부 처리용 Consumer는 ApplicationEventPublisher/EventListener 패턴으로 대체되었습니다.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, GenericRecord> avroKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, GenericRecord> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(avroConsumerFactory());
        return factory;
    }

    // ----------- Producer Factory --------------

    /**
     * OrderEvent 전용 Producer Factory
     * 
     * JsonSerializer를 사용하여 OrderEvent를 JSON으로 직렬화합니다.
     * 타입 정보를 헤더에 추가하여 역직렬화 시 타입을 알 수 있도록 합니다.
     */
    @Bean
    public ProducerFactory<String, OrderEvent> orderEventProducerFactory() {
        // 직렬화, 역직렬화를 위한 타입 정보 헤더 추가
        Map<String, Object> props = Map.ofEntries(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class),
                Map.entry(JsonSerializer.ADD_TYPE_INFO_HEADERS, true)
        );
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * OrderEvent 전용 Kafka Template
     */
    @Bean
    public KafkaTemplate<String, OrderEvent> kafkaTemplate() {
        return new KafkaTemplate<>(orderEventProducerFactory());
    }

    /**
     * ProductEvent 전용 Producer Factory
     * 
     * JsonSerializer를 사용하여 ProductEvent를 JSON으로 직렬화합니다.
     * 타입 정보를 헤더에 추가하여 역직렬화 시 타입을 알 수 있도록 합니다.
     */
    @Bean
    public ProducerFactory<String, ProductEvent> productEventProducerFactory() {
        // 직렬화, 역직렬화를 위한 타입 정보 헤더 추가
        Map<String, Object> props = Map.ofEntries(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class),
                Map.entry(JsonSerializer.ADD_TYPE_INFO_HEADERS, true)
        );
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * ProductEvent 전용 Kafka Template
     */
    @Bean
    public KafkaTemplate<String, ProductEvent> productEventKafkaTemplate() {
        return new KafkaTemplate<>(productEventProducerFactory());
    }

    /**
     * Avro Producer Factory
     * 
     * Apache Avro = JSON처럼 생겼지만 훨씬 빠르고, 가볍고,
     * Schema(스키마)를 함께 사용해서 안전하게 직렬화/역직렬화하는 데이터 포맷.
     * 
     * ACKS 설정:
     * - "0": 확인 안함 (가장 빠름)
     * - "1": 리더에게 전달되었는지 확인 (기본값)
     * - "all": 복제본까지 확인 (금융, 결제 시스템에서 주로 사용, 시간이 제일 오래 걸림)
     */
    @Bean
    public ProducerFactory<String, GenericRecord> avroProducerFactory() {
        // Producer 안정성 설정
        Map<String, Object> props = Map.ofEntries(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers), // Kafka 브로커 서버 주소
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class), // Key 직렬화: String 타입
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class), // Value 직렬화: Avro 형식
                Map.entry(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), // Schema Registry URL (Avro 스키마 관리)
                Map.entry(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true), // 스키마 자동 등록 여부
                Map.entry(ProducerConfig.ACKS_CONFIG, "1"), // 리더에게 전달 확인 ("0": 확인 안함, "1": 리더 확인, "all": 복제본까지 확인)
                Map.entry(ProducerConfig.RETRIES_CONFIG, 3), // 전송 실패 시 재시도 횟수
                Map.entry(ProducerConfig.BATCH_SIZE_CONFIG, 16384), // 배치 크기 (16KB) - 여러 메시지를 묶어서 전송하여 성능 향상
                Map.entry(ProducerConfig.LINGER_MS_CONFIG, 10), // 배치 전송 대기 시간 (ms) - 10ms 동안 메시지를 모아서 배치로 전송
                Map.entry(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy") // 압축 알고리즘: none, lz4, gzip, zstd, snappy (snappy: 빠른 압축/해제)
        );
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Avro Kafka Template
     */
    @Bean
    public KafkaTemplate<String, GenericRecord> avroKafkaTemplate() {
        return new KafkaTemplate<>(avroProducerFactory());
    }

    /**
     * JSON Producer Factory (범용)
     * 
     * ProductClickEventDTO 등 JSON 형식의 DTO를 발행하기 위한 Factory입니다.
     * 타입 정보를 헤더에 추가하지 않아 다른 시스템과의 호환성이 좋습니다.
     */
    @Bean
    public ProducerFactory<String, Object> jsonProducerFactory() {
        Map<String, Object> props = Map.ofEntries(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class),
                Map.entry(JsonSerializer.ADD_TYPE_INFO_HEADERS, false) // 타입 정보 헤더 추가 안함 (호환성)
        );
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * JSON Kafka Template (범용)
     * 
     * ProductClickEventDTO 등 JSON 형식의 메시지를 발행하기 위한 Template입니다.
     */
    @Bean("jsonKafkaTemplate")
    public KafkaTemplate<String, Object> jsonKafkaTemplate() {
        return new KafkaTemplate<>(jsonProducerFactory());
    }
}
