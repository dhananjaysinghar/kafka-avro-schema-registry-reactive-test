package com.kafka.test.controller;

import com.test.Booking;
import com.test.enums.BookingStatusType;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

@Slf4j
@RestController
@RequestMapping("/book")
public class BookingTestController {

    @Autowired
    private KafkaSender<String, Booking> kafkaSender;

    @Value("${BOOKING_TOPIC}")
    private String bookingTopic;

    @GetMapping
    public Mono<String> book() {
        Booking booking = new Booking();
        booking.setBookingNumber(getBookingNo());
        booking.setBookingStatus(BookingStatusType.CONFIRMED);

        kafkaSender.send(publishMsg(bookingTopic, booking))
                .doOnNext(result -> log.info("Sender result for booking {} : {} result:{}", booking.getBookingNumber(), booking, result))
                .doOnError(ex -> log.warn("Failed producing record to Kafka, booking ID: {}", booking.getBookingNumber(), ex))
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(10)))
                .doOnError(ex -> log.error("Failed producing record to Kafka after all retries, booking ID: {}", booking.getBookingNumber(), ex))
                .subscribe();

        return Mono.just("Successfully sent data to kafka");
    }

    private <T> Mono<SenderRecord<String, Booking, T>> publishMsg(String topicName, Booking booking) {
        ProducerRecord<String, Booking> record = new ProducerRecord<>(topicName, booking.getBookingNumber(), booking);
        record.headers().add("Authorization", ("Bearer " + "test").getBytes(StandardCharsets.UTF_8));
        return Mono.fromSupplier(() -> SenderRecord.create(record, null));
    }

    public String getBookingNo() {
        String id = UUID.randomUUID().toString().replace("-", "");
        return "M" + id.substring(0, Math.min(id.length(), 10)).toUpperCase();
    }
}
