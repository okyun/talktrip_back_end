package com.talktrip.talktrip.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * 테스트 환경에서 Kafka 설정을 완화하는 설정
 * 실제 운영 환경에서는 사용하지 않습니다.
 * 
 * 테스트 환경에서는 EmbeddedKafka 또는 Mock을 사용할 수 있습니다.
 */
@TestConfiguration
@Profile("test")
public class TestKafkaConfig {

    /**
     * 테스트용 Kafka Consumer Factory
     * 
     * 테스트 환경에서는 실제 Kafka 서버 없이도 테스트할 수 있도록
     * Mock 또는 EmbeddedKafka를 사용할 수 있습니다.
     */
    @Bean
    public ConsumerFactory<String, Object> testAvroConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        
        // 테스트 환경에서는 기본값 사용 (EmbeddedKafka 또는 Mock 사용 시)
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                System.getProperty("spring.kafka.bootstrap-servers", "localhost:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 
                System.getProperty("spring.kafka.properties.schema.registry.url", "http://localhost:8081"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put("specific.avro.reader", true);
        
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * 테스트용 Kafka Listener Container Factory
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> testKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(testAvroConsumerFactory());
        
        // 테스트 환경에서는 자동 커밋 사용 (간소화)
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }

    /**
     * 테스트용 Kafka Producer Factory
     */
    @Bean
    public ProducerFactory<String, Object> testAvroProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                System.getProperty("spring.kafka.bootstrap-servers", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 
                System.getProperty("spring.kafka.properties.schema.registry.url", "http://localhost:8081"));
        
        // 테스트 환경에서는 안전한 기본값 사용
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * 테스트용 Kafka Template
     */
    @Bean
    public KafkaTemplate<String, Object> testKafkaTemplate() {
        return new KafkaTemplate<>(testAvroProducerFactory());
    }
}


