package com.kafka.test.event.producer;

import com.kafka.test.config.kafka.configuration.KafkaProducerDefaultConfiguration;
import com.test.Booking;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProducerConfig extends KafkaProducerDefaultConfiguration<String, Booking> {
}
