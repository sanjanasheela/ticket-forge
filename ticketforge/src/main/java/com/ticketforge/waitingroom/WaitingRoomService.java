package com.ticketforge.waitingroom;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.ticketforge.auth.TokenService;
import com.ticketforge.auth.TokenService.QueueAccess;
import com.ticketforge.catalog.CatalogApi;
import com.ticketforge.catalog.ShowRules;
import com.ticketforge.shared.ApiException;
import com.ticketforge.shared.Sql;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Token-gated FIFO waiting room (spec §9). Postgres is the source of truth; every second the
 * scheduler admits the next {@code admit_rate_per_sec} users and refreshes an in-memory snapshot
 * of who is still waiting, so "people ahead of you" costs no query per user.
 */
@Service
public class WaitingRoomService {

    private static final Logger log = LoggerFactory.getLogger(WaitingRoomService.class);

    private final JdbcClient jdbc;
    private final CatalogApi catalog;
    private final TokenService tokens;
    private final Clock clock;
    private final Duration accessTtl;
    private final Counter admitted;
    /** show id -> sorted positions of users still waiting */
    private final Map<Long, long[]> waiting = new ConcurrentHashMap<>();
    private final Map<Long, Integer> admitRates = new ConcurrentHashMap<>();
    private final Map<Long, Instant> saleOpens = new ConcurrentHashMap<>();

    WaitingRoomService(JdbcClient jdbc, CatalogApi catalog, TokenService tokens, Clock clock, MeterRegistry meters,
            @Value("${ticketforge.waitingroom.access-token-ttl}") Duration accessTtl) {
        this.jdbc = jdbc;
        this.catalog = catalog;
        this.tokens = tokens;
        this.clock = clock;
        this.accessTtl = accessTtl;
        this.admitted = Counter.builder("ticketforge.queue.admitted").register(meters);
        Gauge.builder("ticketforge.queue.waiting", waiting, w -> w.values().stream().mapToInt(a -> a.length).sum())
                .register(meters);
    }

    public record QueueStatus(long showId, long position, int ahead, boolean admitted, long estimatedWaitSec,
            Instant saleOpensAt) {
    }

    public QueueStatus join(long showId, UUID userId) {
        ShowRules show = catalog.rules(showId);
        if (!show.queueEnabled()) {
            throw ApiException.badRequest("QUEUE_NOT_ENABLED", "This show has no waiting room; book directly");
        }
        if (!show.active() || !clock.instant().isBefore(show.startsAt())) {
            throw new ApiException(HttpStatus.FORBIDDEN, "SALE_CLOSED", "Sales for this show are closed");
        }
        jdbc.sql("""
                        INSERT INTO queue_entries (show_id, user_id, joined_at) VALUES (:s, :u, :now)
                        ON CONFLICT (show_id, user_id) DO NOTHING""")
                .param("s", showId).param("u", userId).param("now", Sql.ts(clock.instant()))
                .update();
        remember(show);
        return status(showId, userId);
    }

    public QueueStatus status(long showId, UUID userId) {
        var row = jdbc.sql("SELECT position, admitted_at FROM queue_entries WHERE show_id = :s AND user_id = :u")
                .param("s", showId).param("u", userId)
                .query((rs, i) -> new Object[] {rs.getLong(1), rs.getTimestamp(2) != null})
                .optional()
                .orElseThrow(() -> ApiException.notFound("NOT_IN_QUEUE", "Join the queue first"));
        long position = (long) row[0];
        boolean isAdmitted = (boolean) row[1];
        return statusOf(showId, position, isAdmitted);
    }

    /**
     * From the snapshot only (no query while people wait): used for the SSE pushes. Admission is
     * FIFO, so admitted users always hold the smallest positions: below the first waiting
     * position means admitted. Anything else is still waiting, including users who joined after
     * the last snapshot (their positions are larger than everyone's in it).
     */
    QueueStatus statusFromSnapshot(long showId, long position) {
        long[] w = waiting.get(showId);
        if (w == null || w.length == 0) {
            return status(showId, position); // nobody waiting per snapshot (or none yet): ask the DB once
        }
        return statusOf(showId, position, position < w[0]);
    }

    private QueueStatus status(long showId, long position) {
        boolean isAdmitted = jdbc.sql("SELECT admitted_at IS NOT NULL FROM queue_entries WHERE show_id = :s AND position = :p")
                .param("s", showId).param("p", position).query(Boolean.class).optional().orElse(false);
        return statusOf(showId, position, isAdmitted);
    }

    private QueueStatus statusOf(long showId, long position, boolean isAdmitted) {
        int ahead = isAdmitted ? 0 : indexOf(waiting.getOrDefault(showId, new long[0]), position);
        int rate = Math.max(1, admitRates.getOrDefault(showId, 1));
        Instant opens = saleOpens.computeIfAbsent(showId, id -> catalog.rules(id).saleOpensAt());
        long untilSale = Math.max(0, Duration.between(clock.instant(), opens).toSeconds());
        long wait = isAdmitted ? 0 : untilSale + (ahead + rate - 1) / rate;
        return new QueueStatus(showId, position, ahead, isAdmitted, wait, opens);
    }

    private void remember(ShowRules show) {
        admitRates.put(show.showId(), show.admitRatePerSec());
        saleOpens.put(show.showId(), show.saleOpensAt());
    }

    /** Number of waiting users with a smaller position. */
    private static int indexOf(long[] sorted, long position) {
        int i = Arrays.binarySearch(sorted, position);
        return i >= 0 ? i : -(i + 1);
    }

    public QueueAccess access(long showId, UUID userId) {
        QueueStatus status = status(showId, userId);
        if (!status.admitted()) {
            throw new ApiException(HttpStatus.CONFLICT, "NOT_ADMITTED_YET", "Still in the queue",
                    Map.of("ahead", status.ahead(), "estimatedWaitSec", status.estimatedWaitSec()));
        }
        return tokens.issueQueueAccess(userId, showId, accessTtl);
    }

    @Scheduled(fixedDelay = 1000)
    void tick() {
        try {
            Instant now = clock.instant();
            for (ShowRules show : catalog.queueEnabledShowsOnSale(now)) {
                remember(show);
                int n = jdbc.sql("""
                                UPDATE queue_entries SET admitted_at = :now
                                WHERE (show_id, user_id) IN (
                                    SELECT show_id, user_id FROM queue_entries
                                    WHERE show_id = :s AND admitted_at IS NULL
                                    ORDER BY position LIMIT :n
                                    FOR UPDATE SKIP LOCKED)""")
                        .param("now", Sql.ts(now)).param("s", show.showId()).param("n", show.admitRatePerSec())
                        .update();
                if (n > 0) {
                    admitted.increment(n);
                }
            }
            refreshSnapshots();
        } catch (RuntimeException e) {
            log.warn("Waiting room tick failed: {}", e.toString());
        }
    }

    private void refreshSnapshots() {
        Map<Long, long[]> fresh = new ConcurrentHashMap<>();
        Map<Long, java.util.List<Long>> byShow = jdbc.sql(
                        "SELECT show_id, position FROM queue_entries WHERE admitted_at IS NULL ORDER BY show_id, position")
                .query((rs, i) -> new long[] {rs.getLong(1), rs.getLong(2)})
                .list().stream()
                .collect(Collectors.groupingBy(r -> r[0], Collectors.mapping(r -> r[1], Collectors.toList())));
        byShow.forEach((show, positions) -> fresh.put(show, positions.stream().mapToLong(Long::longValue).toArray()));
        Set<Long> gone = Set.copyOf(waiting.keySet());
        waiting.putAll(fresh);
        gone.stream().filter(s -> !fresh.containsKey(s)).forEach(waiting::remove);
    }
}
