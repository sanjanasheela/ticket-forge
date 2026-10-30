package com.ticketforge.verification;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * The invariants that live in reservation_db, as SQL (seat and GA-counter integrity). The two payment
 * invariants moved to booking-service, and the ones spanning both services are checked by the end-to-end suite.
 * The spec §4 invariants as SQL. Each query counts violations; each runs as one statement, so it
 * sees one consistent snapshot even while the system is under load. The same queries live in
 * scripts/verify-invariants.sql for after a k6 run.
 */
@Component
@RestController
class InvariantChecker {

    private static final Logger log = LoggerFactory.getLogger(InvariantChecker.class);

    static final Map<String, String> INVARIANTS = new LinkedHashMap<>();

    static {
        INVARIANTS.put("seat_sold_twice", """
                SELECT count(*) FROM (
                    SELECT hi.show_id, hi.venue_seat_id FROM hold_items hi JOIN holds h ON h.id = hi.hold_id
                    WHERE h.status = 'CONFIRMED' AND hi.venue_seat_id IS NOT NULL
                    GROUP BY 1, 2 HAVING count(*) > 1) x""");
        INVARIANTS.put("booked_seat_without_confirmed_hold", """
                SELECT count(*) FROM show_seats s LEFT JOIN holds h ON h.id = s.hold_id
                WHERE s.status = 'BOOKED' AND (h.id IS NULL OR h.status <> 'CONFIRMED')""");
        INVARIANTS.put("confirmed_seat_not_booked", """
                SELECT count(*) FROM hold_items hi JOIN holds h ON h.id = hi.hold_id
                LEFT JOIN show_seats s ON s.show_id = hi.show_id AND s.venue_seat_id = hi.venue_seat_id
                WHERE h.status = 'CONFIRMED' AND hi.venue_seat_id IS NOT NULL
                  AND (s.status IS DISTINCT FROM 'BOOKED' OR s.hold_id IS DISTINCT FROM h.id)""");
        INVARIANTS.put("ga_counter_mismatch", """
                WITH items AS (
                    SELECT hi.show_id, hi.tier_id, COALESCE(hi.ga_shard, -1) AS shard,
                           sum(hi.quantity) FILTER (WHERE h.status = 'HELD') AS held,
                           sum(hi.quantity) FILTER (WHERE h.status = 'CONFIRMED') AS sold
                    FROM hold_items hi JOIN holds h ON h.id = hi.hold_id
                    WHERE hi.venue_seat_id IS NULL GROUP BY 1, 2, 3),
                counters AS (
                    SELECT g.show_id, g.tier_id, -1 AS shard, g.held, g.sold
                    FROM ga_inventory g JOIN shows s ON s.id = g.show_id WHERE s.ga_shards = 0
                    UNION ALL
                    SELECT show_id, tier_id, shard, held, sold FROM ga_inventory_shards)
                SELECT count(*) FROM counters c
                LEFT JOIN items i ON i.show_id = c.show_id AND i.tier_id = c.tier_id AND i.shard = c.shard
                WHERE c.held <> COALESCE(i.held, 0) OR c.sold <> COALESCE(i.sold, 0)""");
    }

    private final JdbcClient jdbc;
    private final Map<String, AtomicLong> last = new ConcurrentHashMap<>();

    InvariantChecker(JdbcClient jdbc, MeterRegistry meters) {
        this.jdbc = jdbc;
        INVARIANTS.keySet().forEach(name -> {
            AtomicLong value = new AtomicLong();
            last.put(name, value);
            Gauge.builder("ticketforge.invariant.violations", value, AtomicLong::get)
                    .tag("invariant", name).register(meters);
        });
    }

    @Scheduled(fixedDelayString = "${ticketforge.invariants.check-interval}", initialDelay = 5000)
    void check() {
        try {
            run();
        } catch (RuntimeException e) {
            log.warn("Invariant check failed to run: {}", e.toString());
        }
    }

    Map<String, Long> run() {
        Map<String, Long> result = new LinkedHashMap<>();
        INVARIANTS.forEach((name, sql) -> {
            long n = jdbc.sql(sql).query(Long.class).single();
            last.get(name).set(n);
            result.put(name, n);
            if (n > 0) {
                log.error("INVARIANT VIOLATED invariant={} count={}", name, n);
            }
        });
        return result;
    }

    @GetMapping("/admin/invariants")
    Map<String, Long> now() {
        return run();
    }
}
