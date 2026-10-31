package com.ticketforge.booking;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import com.ticketforge.contracts.SagaMessages.ConfirmHoldReply;
import com.ticketforge.contracts.Topics;
import com.ticketforge.messaging.MessageBus;

import tools.jackson.databind.json.JsonMapper;

/** Receives reservation-service's answers to ConfirmHold and moves the saga forward. */
@Component
class ReplyListener {

    private final CheckoutSaga saga;
    private final JsonMapper json;
    private final MessageBus bus;

    ReplyListener(CheckoutSaga saga, JsonMapper json, MessageBus bus) {
        this.saga = saga;
        this.json = json;
        this.bus = bus;
    }

    @PostConstruct
    void subscribe() {
        bus.subscribe("booking-service", Topics.SAGA_RESERVATION_REPLIES, this::onReply);
    }

    void onReply(String payload) {
        saga.onReply(json.readValue(payload, ConfirmHoldReply.class));
    }
}
