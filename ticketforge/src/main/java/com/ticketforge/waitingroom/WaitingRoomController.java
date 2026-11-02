package com.ticketforge.waitingroom;

import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.ticketforge.auth.TokenService.QueueAccess;
import com.ticketforge.shared.CurrentUser;
import com.ticketforge.waitingroom.WaitingRoomService.QueueStatus;

@RestController
@RequestMapping("/shows/{showId}/queue")
class WaitingRoomController {

    private final WaitingRoomService queue;
    private final QueueStreams streams;

    WaitingRoomController(WaitingRoomService queue, QueueStreams streams) {
        this.queue = queue;
        this.streams = streams;
    }

    @PostMapping
    QueueStatus join(@AuthenticationPrincipal Jwt jwt, @PathVariable long showId) {
        return queue.join(showId, CurrentUser.id(jwt));
    }

    @GetMapping
    QueueStatus status(@AuthenticationPrincipal Jwt jwt, @PathVariable long showId) {
        return queue.status(showId, CurrentUser.id(jwt));
    }

    /** Send the returned token as X-Queue-Token when creating holds. */
    @GetMapping("/access")
    QueueAccess access(@AuthenticationPrincipal Jwt jwt, @PathVariable long showId) {
        return queue.access(showId, CurrentUser.id(jwt));
    }

    @GetMapping(path = "/stream", produces = "text/event-stream")
    SseEmitter stream(@AuthenticationPrincipal Jwt jwt, @PathVariable long showId) {
        return streams.subscribe(showId, CurrentUser.id(jwt));
    }
}
