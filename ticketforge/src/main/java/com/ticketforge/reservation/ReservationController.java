package com.ticketforge.reservation;

import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.ticketforge.reservation.HoldViews.HoldRequest;
import com.ticketforge.reservation.HoldViews.HoldView;
import com.ticketforge.shared.CurrentUser;

import jakarta.validation.Valid;

@RestController
class ReservationController {

    private final ReservationService reservations;

    ReservationController(ReservationService reservations) {
        this.reservations = reservations;
    }

    @PostMapping("/shows/{showId}/holds")
    @ResponseStatus(HttpStatus.CREATED)
    HoldView hold(@AuthenticationPrincipal Jwt jwt, @PathVariable long showId, @Valid @RequestBody HoldRequest body,
            @RequestHeader(name = "X-Queue-Token", required = false) String queueToken) {
        return reservations.createHold(CurrentUser.id(jwt), showId, body, queueToken);
    }

    @GetMapping("/holds/{id}")
    HoldView get(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        return reservations.getHold(id, CurrentUser.id(jwt));
    }

    @DeleteMapping("/holds/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    void release(@AuthenticationPrincipal Jwt jwt, @PathVariable UUID id) {
        reservations.release(id, CurrentUser.id(jwt));
    }
}
