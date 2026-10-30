package com.ticketforge.waitingroom;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.ticketforge.waitingroom.WaitingRoomService.QueueStatus;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/** Pushes each waiting user's position every 2 s, computed from the in-memory snapshot. */
@Component
class QueueStreams {

    private record Subscriber(long showId, long position, SseEmitter emitter) {
    }

    private final WaitingRoomService queue;
    private final Set<Subscriber> subscribers = ConcurrentHashMap.newKeySet();

    QueueStreams(WaitingRoomService queue, MeterRegistry meters) {
        this.queue = queue;
        Gauge.builder("ticketforge.sse.connections", subscribers, Set::size).tag("stream", "queue").register(meters);
    }

    SseEmitter subscribe(long showId, UUID userId) {
        QueueStatus status = queue.status(showId, userId);
        SseEmitter emitter = new SseEmitter(Duration.ofMinutes(60).toMillis());
        Subscriber sub = new Subscriber(showId, status.position(), emitter);
        emitter.onCompletion(() -> subscribers.remove(sub));
        emitter.onTimeout(() -> subscribers.remove(sub));
        emitter.onError(e -> subscribers.remove(sub));
        subscribers.add(sub);
        send(sub, status);
        return emitter;
    }

    @Scheduled(fixedDelay = 2000)
    void push() {
        if (subscribers.isEmpty()) {
            return;
        }
        // one virtual thread per subscriber: a slow client must not delay everyone behind it
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Subscriber sub : subscribers) {
                pool.submit(() -> send(sub, queue.statusFromSnapshot(sub.showId(), sub.position())));
            }
        }
    }

    private void send(Subscriber sub, QueueStatus status) {
        try {
            sub.emitter().send(SseEmitter.event().name("queue").data(status, MediaType.APPLICATION_JSON));
            if (status.admitted()) {
                sub.emitter().complete(); // client now fetches its access token
            }
        } catch (IOException | IllegalStateException e) {
            subscribers.remove(sub);
        }
    }
}
