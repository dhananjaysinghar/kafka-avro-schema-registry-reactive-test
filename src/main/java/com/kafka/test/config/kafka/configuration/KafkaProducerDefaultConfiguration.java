package com.kafka.test.config.kafka.configuration;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.micrometer.core.instrument.Metrics;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.MicrometerProducerListener;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.internals.ProducerFactory;

@Getter
@Setter
@Slf4j
public class KafkaProducerDefaultConfiguration<K, V> {

    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${kafka.client-id}")
    private String clientId;

    @Value("${kafka.username:}")
    private String username;
    @Value("${kafka.password:}")
    private String password;

    @Value("${kafka.login-module:org.apache.kafka.common.security.plain.PlainLoginModule}")
    private String loginModule;
    @Value("${kafka.sasl-mechanism:PLAIN}")
    private String saslMechanism;
    @Value("${kafka.security-protocol:SASL_SSL}")
    private String securityProtocol;

    @Value("${kafka.schema-registry.url}")
    private String schemaRegistryUrl;
    @Value("${kafka.schema-registry.username:#{null}}")
    private String schemaRegistryUsername;
    @Value("${kafka.schema-registry.password:#{null}}")
    private String schemaRegistryPassword;

    @Bean
    public KafkaSender<K, V> kafkaSender() {
        return KafkaSender.create(producerFactory(), kafkaSenderOptions());

    }

    @SuppressWarnings("unchecked")
    private ProducerFactory producerFactory() {
        return new ProducerFactory() {
            @Override
            public <S1, S2> Producer<S1, S2> createProducer(SenderOptions<S1, S2> senderOptions) {
                org.springframework.kafka.core.ProducerFactory<K, V> producerFactory = new DefaultKafkaProducerFactory<>(senderOptions.producerProperties());
                producerFactory.addListener(new MicrometerProducerListener<>(Metrics.globalRegistry));
                return (Producer<S1, S2>) producerFactory.createProducer();
            }
        };
    }

    private SenderOptions<K, V> kafkaSenderOptions() {
        return SenderOptions.create(kafkaProducerProperties());
    }


    private Map<String, Object> kafkaProducerProperties() {
        Map<String, Object> kafkaPropertiesMap = new HashMap<>();
        kafkaPropertiesMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaPropertiesMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaPropertiesMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        kafkaPropertiesMap.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        kafkaPropertiesMap.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        kafkaPropertiesMap.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, schemaRegistryUsername + ":" + schemaRegistryPassword);
        kafkaPropertiesMap.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        return kafkaPropertiesMap;
    }


}
