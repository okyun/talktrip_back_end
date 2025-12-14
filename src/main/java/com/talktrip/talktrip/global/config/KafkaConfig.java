package com.talktrip.talktrip.global.config;

import com.talktrip.talktrip.domain.kafka.dto.order.OrderEvent;
import com.talktrip.talktrip.domain.kafka.dto.product.ProductEvent;
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

import java.util.HashMap;
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
        Map<String, Object> props = new HashMap<>();
        
        // ErrorHandlingDeserializer - 역직렬화 실패 시 애플리케이션 종료 방지, 잘못된 JSON 방식 처리
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        
        // 메시지 포맷이 잘못되어도 consumer 중단하지 않는 형태
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*"); // 전체 path 허용
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        
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
        Map<String, Object> props = new HashMap<>();
        
        // ErrorHandlingDeserializer - 역직렬화 실패 시 애플리케이션 종료 방지, 잘못된 JSON 방식 처리
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        
        // 메시지 포맷이 잘못되어도 consumer 중단하지 않는 형태
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, ProductEvent.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*"); // 전체 path 허용
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        
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
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);
        
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
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Avro Kafka Listener Container Factory
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
    public ProducerFactory<String, Object> orderEventProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        
        // 직렬화, 역직렬화를 위한 타입 정보 헤더 추가
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, true);
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * OrderEvent 전용 Kafka Template
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(orderEventProducerFactory());
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
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        
        // Producer 안정성 설정
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // 리더에게 전달 확인
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // 압축 알고리즘: none, lz4, gzip, zstd, snappy
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Avro Kafka Template
     */
    @Bean
    public KafkaTemplate<String, GenericRecord> avroKafkaTemplate() {
        return new KafkaTemplate<>(avroProducerFactory());
    }
}
