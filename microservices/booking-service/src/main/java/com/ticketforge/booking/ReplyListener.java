package com.ticketforge.booking;

import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.FixedBackOff;

import com.ticketforge.contracts.SagaMessages.ConfirmHoldReply;
import com.ticketforge.contracts.Topics;

import tools.jackson.databind.json.JsonMapper;

/** Receives reservation-service's answers to ConfirmHold and moves the saga forward. */
@Component
class ReplyListener {

    private final CheckoutSaga saga;
    private final JsonMapper json;

    ReplyListener(CheckoutSaga saga, JsonMapper json) {
        this.saga = saga;
        this.json = json;
    }

    @KafkaListener(topics = Topics.SAGA_RESERVATION_REPLIES, groupId = "booking-service")
    void onReply(String payload) {
        saga.onReply(json.readValue(payload, ConfirmHoldReply.class));
    }

    /** A reply must not be skipped: retry forever, one second apart, instead of Boot's default of dropping it after 9 tries. */
    @Bean
    CommonErrorHandler replyErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1_000, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
