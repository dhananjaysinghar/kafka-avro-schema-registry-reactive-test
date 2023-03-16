package com.kafka.test.service;

import com.test.Booking;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.util.retry.Retry;

@Service
@Slf4j
public class KafkaConsumerService {

    @Autowired
    private KafkaReceiver<String, Booking> kafkaReceiver;


    /**
     * method to consume the messages and start the consumer.
     */
    @EventListener(ApplicationStartedEvent.class)
    public Disposable startKafkaConsumer() {
        return kafkaReceiver.receive().retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1)).onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> Exceptions.propagate(retrySignal.failure()))).doOnError(error -> log.error("Error occurred while consuming service plan event from kafka : {} ", error.getMessage())).subscribe(receiverRecord -> {
            receiverRecord.receiverOffset().acknowledge();
            log.info("Successfully consumed service plan events from Kafka : {}", receiverRecord.value());
        });
    }
}
