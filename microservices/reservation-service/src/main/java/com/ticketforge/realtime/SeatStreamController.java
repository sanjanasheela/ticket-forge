package com.ticketforge.realtime;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

@RestController
class SeatStreamController {

    private final SeatStreams streams;

    SeatStreamController(SeatStreams streams) {
        this.streams = streams;
    }

    @GetMapping(path = "/shows/{id}/seats/stream", produces = "text/event-stream")
    SseEmitter stream(@PathVariable long id) {
        return streams.subscribe(id);
    }
}
