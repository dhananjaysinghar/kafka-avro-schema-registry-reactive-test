package com.kafka.test.config.kafka.configuration;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.micrometer.core.instrument.Metrics;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.internals.ConsumerFactory;

@Getter
@Setter
@Slf4j
public class KafkaConsumerDefaultConfiguration<K, V> {

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

    @Value("${kafka.consumer.consumer-group}")
    private String consumerGroup;
    @Value("${kafka.consumer.topic}")
    private String consumerTopicName;
    @Value("${kafka.consumer.offset-auto-reset:latest}")
    private String consumerOffsetAutoReset;
    @Value("${kafka.consumer.max-poll-records:100}")
    private String consumerMaxPollRecords;
    @Value("${kafka.consumer.max-poll-timeout:5000}")
    private long pollTimeout;
    @Value("${kafka.consumer.max-fetch-size-bytes}")
    private Integer maxRequestSizeBytes;

    @Value("${kafka.schema-registry.url}")
    private String schemaRegistryUrl;
    @Value("${kafka.schema-registry.username:#{null}}")
    private String schemaRegistryUsername;
    @Value("${kafka.schema-registry.password:#{null}}")
    private String schemaRegistryPassword;


    @Bean
    public KafkaReceiver<K, V> kafkaReceiver() {
        return KafkaReceiver.create(consumerFactory(), kafkaReceiverOptions());

    }

    @SuppressWarnings("unchecked")
    private ConsumerFactory consumerFactory() {
        return new ConsumerFactory() {
            @Override
            public <S1, S2> Consumer<S1, S2> createConsumer(ReceiverOptions<S1, S2> receiverOptions) {
                org.springframework.kafka.core.ConsumerFactory<K, V> consumerFactory = new DefaultKafkaConsumerFactory<>(receiverOptions.consumerProperties());
                consumerFactory.addListener(new MicrometerConsumerListener<>(Metrics.globalRegistry));
                return (Consumer<S1, S2>) consumerFactory.createConsumer();

            }
        };
    }

    private ReceiverOptions<K, V> kafkaReceiverOptions() {
        ReceiverOptions<K, V> options = ReceiverOptions.create(kafkaConsumerProperties());
        return options.pollTimeout(Duration.ofMillis(pollTimeout)).subscription(List.of(consumerTopicName));
    }

    private Map<String, Object> kafkaConsumerProperties() {
        Map<String, Object> kafkaPropertiesMap = new HashMap<>();
        kafkaPropertiesMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaPropertiesMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        kafkaPropertiesMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        kafkaPropertiesMap.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        kafkaPropertiesMap.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, KafkaAvroDeserializer.class);
        kafkaPropertiesMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, consumerOffsetAutoReset);
        kafkaPropertiesMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaPropertiesMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerMaxPollRecords);
        kafkaPropertiesMap.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxRequestSizeBytes);
        kafkaPropertiesMap.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        kafkaPropertiesMap.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        kafkaPropertiesMap.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, schemaRegistryUsername + ":" + schemaRegistryPassword);
        kafkaPropertiesMap.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        kafkaPropertiesMap.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        kafkaPropertiesMap.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        return kafkaPropertiesMap;
    }
}
