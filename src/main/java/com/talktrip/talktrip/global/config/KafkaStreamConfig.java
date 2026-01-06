package com.talktrip.talktrip.global.config;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Streams 설정
 *
 * Kafka Streams 애플리케이션의 공통 설정을 관리합니다.
 * KafkaProperties를 사용하여 Spring Boot의 자동 설정을 활용합니다.
 */
@Configuration
public class KafkaStreamConfig {

    private final KafkaProperties kafkaProperties;

    @Value("${spring.kafka.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    public KafkaStreamConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * 기본 Kafka Streams 설정
     *
     * KafkaProperties를 사용하여 Spring Boot의 자동 설정을 활용하고,
     * 기본 Key/Value Serde를 String으로 설정합니다.
     *
     * @return KafkaStreamsConfiguration
     */
    @Bean("defaultKafkaStreamsConfig")
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig() {
        Map<String, Object> props = kafkaProperties.getStreams().buildProperties(null);

        // application.id가 없으면 기본값 설정
        if (!props.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "talktrip-streams-app");
        }
        
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return new KafkaStreamsConfiguration(props);
    }

    /**
     * Avro Serde Bean,(ProductClickProcessor에서 사용 중)
     *
     * Kafka Streams에서 Avro 형식의 메시지를 읽고 쓰기 위한 GenericAvroSerde를 제공합니다.
     * Schema Registry URL이 이미 설정되어 있어 각 Processor에서 주입받아 사용할 수 있습니다.
     *
     * @return 설정된 GenericAvroSerde
     */
    @Bean
    public GenericAvroSerde genericAvroSerde() {
        GenericAvroSerde avroSerde = new GenericAvroSerde();
        Map<String, Object> serdeConfig = new HashMap<>();
        serdeConfig.put("schema.registry.url", schemaRegistryUrl);
        avroSerde.configure(serdeConfig, false); // false = value serde
        return avroSerde;
    }
}