package com.ticketforge.realtime;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.ticketforge.inventory.GaChanged;
import com.ticketforge.inventory.InventoryService;
import com.ticketforge.inventory.SeatsChanged;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;

/**
 * Live seat maps (spec §10). Listens AFTER_COMMIT, so viewers never see a hold that rolled back.
 * Fan-out runs on one thread to keep per-show event order (seq); its queue length is exported,
 * which shows when fan-out falls behind under load. Single-instance only, on purpose: scaling
 * out needs pub/sub (Phase 2).
 */
@Component
class SeatStreams {

    record SeatEvent(long seq, String change, List<Long> seatIds) {
    }

    record GaEvent(long seq, long tierId, int available) {
    }

    record Hello(long seq) {
    }

    private final InventoryService inventory;
    private final Map<Long, Set<SseEmitter>> byShow = new ConcurrentHashMap<>();
    private final Map<Long, AtomicLong> seqs = new ConcurrentHashMap<>();
    private final Set<GaChanged> dirtyGa = ConcurrentHashMap.newKeySet();
    private final ThreadPoolExecutor fanout = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
            r -> new Thread(r, "sse-fanout"));

    SeatStreams(InventoryService inventory, MeterRegistry meters) {
        this.inventory = inventory;
        Gauge.builder("ticketforge.sse.connections", byShow, m -> m.values().stream().mapToInt(Set::size).sum())
                .tag("stream", "seats").register(meters);
        Gauge.builder("ticketforge.sse.fanout.backlog", fanout, e -> e.getQueue().size()).register(meters);
    }

    SseEmitter subscribe(long showId) {
        SseEmitter emitter = new SseEmitter(Duration.ofMinutes(30).toMillis());
        Set<SseEmitter> set = byShow.computeIfAbsent(showId, id -> ConcurrentHashMap.newKeySet());
        Runnable remove = () -> set.remove(emitter);
        emitter.onCompletion(remove);
        emitter.onTimeout(remove);
        emitter.onError(e -> remove.run());
        set.add(emitter);
        long seq = seqs.computeIfAbsent(showId, id -> new AtomicLong()).get();
        // The client buffers events after this hello, fetches the snapshot, then applies the buffer.
        fanout.execute(() -> send(set, emitter, "hello", new Hello(seq)));
        return emitter;
    }

    @TransactionalEventListener
    void on(SeatsChanged e) {
        if (!hasViewers(e.showId())) {
            return;
        }
        fanout.execute(() -> broadcast(e.showId(), "seats",
                new SeatEvent(nextSeq(e.showId()), e.change().name(), e.seatIds())));
    }

    /** GA counts change on every ticket; coalesce them and push at most 4 times a second per tier. */
    @TransactionalEventListener
    void on(GaChanged e) {
        if (hasViewers(e.showId())) {
            dirtyGa.add(e);
        }
    }

    @Scheduled(fixedDelay = 250)
    void flushGa() {
        for (GaChanged ga : Set.copyOf(dirtyGa)) {
            dirtyGa.remove(ga);
            int available = inventory.gaAvailable(ga.showId(), ga.tierId());
            fanout.execute(() -> broadcast(ga.showId(), "ga", new GaEvent(nextSeq(ga.showId()), ga.tierId(), available)));
        }
    }

    @Scheduled(fixedDelay = 15_000)
    void heartbeat() {
        fanout.execute(() -> byShow.values().forEach(set -> set.forEach(em -> {
            try {
                em.send(SseEmitter.event().comment("hb"));
            } catch (IOException | IllegalStateException ex) {
                set.remove(em);
            }
        })));
    }

    private boolean hasViewers(long showId) {
        Set<SseEmitter> set = byShow.get(showId);
        return set != null && !set.isEmpty();
    }

    private long nextSeq(long showId) {
        return seqs.computeIfAbsent(showId, id -> new AtomicLong()).incrementAndGet();
    }

    private void broadcast(long showId, String name, Object data) {
        Set<SseEmitter> set = byShow.get(showId);
        if (set != null) {
            set.forEach(em -> send(set, em, name, data));
        }
    }

    private static void send(Set<SseEmitter> set, SseEmitter em, String name, Object data) {
        try {
            em.send(SseEmitter.event().name(name).data(data, MediaType.APPLICATION_JSON));
        } catch (IOException | IllegalStateException ex) {
            set.remove(em);
        }
    }

    @PreDestroy
    void shutdown() {
        fanout.shutdownNow();
        byShow.values().forEach(set -> set.forEach(SseEmitter::complete));
    }
}
