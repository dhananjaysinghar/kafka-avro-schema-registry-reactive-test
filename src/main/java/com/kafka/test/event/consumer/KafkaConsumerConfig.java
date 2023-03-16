package com.kafka.test.event.consumer;

import com.kafka.test.config.kafka.configuration.KafkaConsumerDefaultConfiguration;
import com.test.Booking;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConsumerConfig extends KafkaConsumerDefaultConfiguration<String, Booking> {
}
