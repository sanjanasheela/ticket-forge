package com.ticketforge.reservation;

import java.time.Clock;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.ticketforge.contracts.SagaMessages.ConfirmHold;
import com.ticketforge.contracts.SagaMessages.ConfirmHoldReply;
import com.ticketforge.contracts.Topics;
import com.ticketforge.messaging.MessageBus;
import com.ticketforge.messaging.Outbox;

import tools.jackson.databind.json.JsonMapper;

/**
 * Participant side of the checkout saga: confirms a hold when booking-service says the money is taken.
 *
 * <p>The decision and its reply are one transaction (state change + outbox row), so the orchestrator
 * can never hear "confirmed" for a hold that is not, nor miss the reply of one that is. The handler is
 * idempotent by state: a repeated command for an already confirmed hold simply answers "confirmed" again.
 */
@Component
class ConfirmHoldListener {

    private static final Logger log = LoggerFactory.getLogger(ConfirmHoldListener.class);

    private final ReservationService reservations;
    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final Outbox outbox;
    private final Clock clock;
    private final JsonMapper json;
    private final MessageBus bus;

    ConfirmHoldListener(ReservationService reservations, JdbcClient jdbc, TransactionTemplate tx, Outbox outbox,
            Clock clock, JsonMapper json, MessageBus bus) {
        this.reservations = reservations;
        this.jdbc = jdbc;
        this.tx = tx;
        this.outbox = outbox;
        this.clock = clock;
        this.json = json;
        this.bus = bus;
    }

    @PostConstruct
    void subscribe() {
        bus.subscribe("reservation-service-saga", Topics.SAGA_RESERVATION_COMMANDS, this::onCommand);
    }

    void onCommand(String payload) {
        ConfirmHold cmd = json.readValue(payload, ConfirmHold.class);
        ConfirmHoldReply reply;
        try {
            // Transaction 1: confirm the hold and record the "confirmed" reply together.
            reply = tx.execute(status -> {
                reservations.confirm(cmd.holdId(), cmd.userId(), clock.instant());
                return reply(cmd, new ConfirmHoldReply(cmd.sagaId(), cmd.holdId(), true, null));
            });
        } catch (HoldNoLongerValidException e) {
            // Transaction 1 rolled back, so nothing changed. Transaction 2 records why. (A failed call through
            // the service proxy marks its transaction rollback-only, so the answer cannot be written in the same one.)
            reply = tx.execute(status -> reply(cmd, notConfirmed(cmd)));
        }
        log.info("ConfirmHold handled hold_id={} confirmed={} reason={}", cmd.holdId(), reply.confirmed(), reply.reason());
    }

    private ConfirmHoldReply reply(ConfirmHold cmd, ConfirmHoldReply reply) {
        outbox.add(Topics.SAGA_RESERVATION_REPLIES, cmd.holdId().toString(), Topics.CONFIRM_HOLD_REPLY, reply);
        return reply;
    }

    /** Either a duplicate of a command we already applied (answer "confirmed" again) or the hold really is gone. */
    private ConfirmHoldReply notConfirmed(ConfirmHold cmd) {
        String status = jdbc.sql("SELECT status FROM holds WHERE id = :id AND user_id = :u")
                .param("id", cmd.holdId()).param("u", cmd.userId()).query(String.class).optional().orElse("MISSING");
        return "CONFIRMED".equals(status)
                ? new ConfirmHoldReply(cmd.sagaId(), cmd.holdId(), true, null)
                : new ConfirmHoldReply(cmd.sagaId(), cmd.holdId(), false, "HOLD_" + status);
    }
}
