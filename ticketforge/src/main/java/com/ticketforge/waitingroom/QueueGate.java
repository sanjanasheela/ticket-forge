package com.ticketforge.waitingroom;

import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import com.ticketforge.auth.TokenService;
import com.ticketforge.catalog.ShowRules;
import com.ticketforge.shared.ApiException;

/** Checked by the hold path: on a queue-enabled show, only users the waiting room admitted may hold. */
@Component
public class QueueGate {

    private final TokenService tokens;

    QueueGate(TokenService tokens) {
        this.tokens = tokens;
    }

    public void check(ShowRules show, UUID userId, String queueToken) {
        if (!show.queueEnabled()) {
            return;
        }
        if (queueToken == null || !tokens.isValidQueueAccess(queueToken, userId, show.showId())) {
            throw new ApiException(HttpStatus.FORBIDDEN, "QUEUE_REQUIRED",
                    "This show uses a waiting room: join the queue and use the access token you get when admitted");
        }
    }
}
