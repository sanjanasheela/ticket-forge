package com.ticketforge.reservation;

import java.sql.ResultSet;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import com.ticketforge.catalog.CatalogApi;
import com.ticketforge.catalog.ShowRules;
import com.ticketforge.inventory.ClaimedSeat;
import com.ticketforge.inventory.GaItem;
import com.ticketforge.inventory.InventoryService;
import com.ticketforge.inventory.LockStrategy;
import com.ticketforge.inventory.RetryableClaimException;
import com.ticketforge.reservation.HoldViews.ActiveHold;
import com.ticketforge.reservation.HoldViews.HoldItem;
import com.ticketforge.reservation.HoldViews.HoldRequest;
import com.ticketforge.reservation.HoldViews.HoldView;
import com.ticketforge.shared.ApiException;
import com.ticketforge.shared.Sql;
import com.ticketforge.waitingroom.QueueGate;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/** Hold lifecycle: create (claim inventory), release, expire, confirm. */
@Service
public class ReservationService {

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);

    private final JdbcClient jdbc;
    private final TransactionTemplate tx;
    private final CatalogApi catalog;
    private final InventoryService inventory;
    private final QueueGate queueGate;
    private final Clock clock;
    private final MeterRegistry meters;
    private final int maxAttempts;
    private final Counter expired;

    ReservationService(JdbcClient jdbc, TransactionTemplate tx, CatalogApi catalog, InventoryService inventory,
            QueueGate queueGate, Clock clock, MeterRegistry meters,
            @Value("${ticketforge.inventory.max-claim-attempts}") int maxAttempts) {
        this.jdbc = jdbc;
        this.tx = tx;
        this.catalog = catalog;
        this.inventory = inventory;
        this.queueGate = queueGate;
        this.clock = clock;
        this.meters = meters;
        this.maxAttempts = maxAttempts;
        this.expired = Counter.builder("ticketforge.holds.expired").register(meters);
    }

    enum Kind { SEATS, GA, BEST_AVAILABLE }

    public HoldView createHold(UUID userId, long showId, HoldRequest req, String queueToken) {
        Instant now = clock.instant();
        ShowRules show = catalog.rules(showId);
        if (!show.saleOpen(now)) {
            throw new ApiException(HttpStatus.FORBIDDEN, "SALE_NOT_OPEN", "Sales for this show are not open");
        }
        queueGate.check(show, userId, queueToken);
        Kind kind = kindOf(req);
        int count = switch (kind) {
            case SEATS -> (int) req.seatIds().stream().distinct().count();
            case GA -> req.quantity();
            case BEST_AVAILABLE -> req.bestAvailable();
        };
        if (count > show.maxSeatsPerHold()) {
            throw new ApiException(HttpStatus.UNPROCESSABLE_CONTENT, "MAX_SEATS_EXCEEDED",
                    "At most " + show.maxSeatsPerHold() + " tickets per booking", Map.of("max", show.maxSeatsPerHold()));
        }

        LockStrategy strategy = inventory.strategy();
        Timer.Sample sample = Timer.start(meters);
        String outcome = "error";
        try {
            for (int attempt = 1; ; attempt++) {
                try {
                    HoldView hold = tx.execute(status -> create(userId, show, req, kind, clock.instant()));
                    outcome = "success";
                    return hold;
                } catch (RetryableClaimException | PessimisticLockingFailureException e) {
                    String reason = e instanceof RetryableClaimException ? "optimistic_conflict" : e.getClass().getSimpleName();
                    meters.counter("ticketforge.hold.retries", "strategy", strategy.name(), "reason", reason).increment();
                    if (attempt >= maxAttempts) {
                        outcome = "contention";
                        throw new ApiException(HttpStatus.SERVICE_UNAVAILABLE, "CONTENTION",
                                "Too many people are booking these seats right now, try again");
                    }
                    backoff(attempt);
                }
            }
        } catch (ApiException e) {
            if (!"contention".equals(outcome)) {
                outcome = e.status() == HttpStatus.CONFLICT ? "conflict" : "rejected";
            }
            throw e;
        } finally {
            sample.stop(meters.timer("ticketforge.hold.claim", "strategy", strategy.name(), "kind", kind.name(), "outcome", outcome));
        }
    }

    private HoldView create(UUID userId, ShowRules show, HoldRequest req, Kind kind, Instant now) {
        Integer maxHolds = show.maxHoldsPerUser();
        if (maxHolds != null) {
            expireOwnStaleHolds(userId, show.showId(), now);
            if (maxHolds > 1) {
                int active = jdbc.sql("""
                                SELECT count(*) FROM holds
                                WHERE show_id = :s AND user_id = :u AND status = 'HELD' AND expires_at > :now""")
                        .param("s", show.showId()).param("u", userId).param("now", Sql.ts(now))
                        .query(Integer.class).single();
                if (active >= maxHolds) {
                    throw holdLimit(maxHolds);
                }
            }
        }

        UUID holdId = UUID.randomUUID();
        Instant expiresAt = now.plus(show.holdTtl());
        try {
            // Inserted before touching seats: the one-hold-per-user unique index rejects cheaply.
            jdbc.sql("""
                            INSERT INTO holds (id, user_id, show_id, status, single_hold, expires_at, currency, created_at)
                            VALUES (:id, :u, :s, 'HELD', :single, :exp, :cur, :now)""")
                    .param("id", holdId).param("u", userId).param("s", show.showId())
                    .param("single", Integer.valueOf(1).equals(maxHolds)).param("exp", Sql.ts(expiresAt))
                    .param("cur", show.currency()).param("now", Sql.ts(now))
                    .update();
        } catch (DuplicateKeyException e) {
            throw holdLimit(1);
        } catch (DataIntegrityViolationException e) {
            // validly signed token for a user that no longer exists (deleted account)
            throw new ApiException(HttpStatus.UNAUTHORIZED, "UNKNOWN_USER", "This account no longer exists");
        }

        Map<Long, Long> prices = catalog.tierPrices(show.showId());
        List<HoldItem> items = new ArrayList<>();
        switch (kind) {
            case SEATS -> inventory.claimSeats(show.showId(), req.seatIds(), holdId, expiresAt, now)
                    .forEach(s -> items.add(seatItem(s, prices)));
            case BEST_AVAILABLE -> inventory.claimBestAvailable(show.showId(), req.tierId(), req.bestAvailable(), holdId, expiresAt, now)
                    .forEach(s -> items.add(seatItem(s, prices)));
            case GA -> {
                GaItem ga = inventory.claimGa(show.showId(), req.tierId(), req.quantity(), show.gaShards());
                insertItem(holdId, show.showId(), ga.tierId(), null, ga.shard(), ga.quantity(), price(prices, ga.tierId()), "GA");
                items.add(new HoldItem(null, ga.tierId(), "GA", ga.quantity(), price(prices, ga.tierId())));
            }
        }
        if (kind != Kind.GA) {
            items.forEach(i -> insertItem(holdId, show.showId(), i.tierId(), i.seatId(), null, 1, i.priceMinor(), i.label()));
        }
        long total = items.stream().mapToLong(i -> i.priceMinor() * i.quantity()).sum();
        jdbc.sql("UPDATE holds SET total_minor = :t WHERE id = :id").param("t", total).param("id", holdId).update();

        log.info("Hold created hold_id={} show_id={} user_id={} kind={} tickets={}", holdId, show.showId(), userId, kind,
                items.stream().mapToInt(HoldItem::quantity).sum());
        return new HoldView(holdId, show.showId(), "HELD", expiresAt, total, show.currency(), items);
    }

    /** A not-yet-swept expired hold must not block the user's next attempt. */
    private void expireOwnStaleHolds(UUID userId, long showId, Instant now) {
        jdbc.sql("""
                        SELECT id FROM holds
                        WHERE show_id = :s AND user_id = :u AND status = 'HELD' AND expires_at <= :now
                        FOR UPDATE SKIP LOCKED""")
                .param("s", showId).param("u", userId).param("now", Sql.ts(now))
                .query(UUID.class).list()
                .forEach(id -> end(id, showId, "EXPIRED", now));
    }

    public HoldView getHold(UUID holdId, UUID userId) {
        return jdbc.sql("SELECT id, show_id, status, expires_at, total_minor, currency FROM holds WHERE id = :id AND user_id = :u")
                .param("id", holdId).param("u", userId)
                .query((rs, i) -> new HoldView(holdId, rs.getLong("show_id"), rs.getString("status"),
                        rs.getTimestamp("expires_at").toInstant(), rs.getLong("total_minor"), rs.getString("currency"),
                        items(holdId)))
                .optional()
                .orElseThrow(ReservationService::holdNotFound);
    }

    public List<HoldItem> items(UUID holdId) {
        return jdbc.sql("SELECT venue_seat_id, tier_id, label, quantity, price_minor FROM hold_items WHERE hold_id = :h ORDER BY id")
                .param("h", holdId)
                .query((rs, i) -> new HoldItem(rs.getObject("venue_seat_id", Long.class), rs.getLong("tier_id"),
                        rs.getString("label"), rs.getInt("quantity"), rs.getLong("price_minor")))
                .list();
    }

    @Transactional
    public void release(UUID holdId, UUID userId) {
        Long showId = jdbc.sql("SELECT show_id FROM holds WHERE id = :id AND user_id = :u")
                .param("id", holdId).param("u", userId).query(Long.class).optional()
                .orElseThrow(ReservationService::holdNotFound);
        if (!end(holdId, showId, "RELEASED", clock.instant())) {
            throw ApiException.conflict("HOLD_NOT_ACTIVE", "This hold is no longer active");
        }
    }

    /** The hold, if it is this user's, still HELD and unexpired. */
    @Transactional
    public Optional<ActiveHold> findActive(UUID holdId, UUID userId, Instant now) {
        return jdbc.sql("""
                        SELECT id, show_id, total_minor, currency, expires_at FROM holds
                        WHERE id = :id AND user_id = :u AND status = 'HELD' AND expires_at > :now""")
                .param("id", holdId).param("u", userId).param("now", Sql.ts(now))
                .query((rs, i) -> new ActiveHold(holdId, rs.getLong("show_id"), rs.getLong("total_minor"),
                        rs.getString("currency"), rs.getTimestamp("expires_at").toInstant()))
                .optional();
    }

    public boolean exists(UUID holdId, UUID userId) {
        return jdbc.sql("SELECT 1 FROM holds WHERE id = :id AND user_id = :u").param("id", holdId).param("u", userId)
                .query(Integer.class).optional().isPresent();
    }

    /**
     * HELD -> CONFIRMED and seats -> BOOKED, inside the caller's transaction. The guarded UPDATE is
     * the single decision point between a late payment and the expiry sweeper: exactly one wins.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void confirm(UUID holdId, UUID userId, Instant now) {
        Long showId = jdbc.sql("""
                        UPDATE holds SET status = 'CONFIRMED', updated_at = :now
                        WHERE id = :id AND user_id = :u AND status = 'HELD' AND expires_at > :now
                        RETURNING show_id""")
                .param("now", Sql.ts(now)).param("id", holdId).param("u", userId)
                .query(Long.class).optional()
                .orElseThrow(() -> new HoldNoLongerValidException("Hold " + holdId + " expired before payment completed"));
        ItemsSplit split = split(holdId);
        if (!inventory.confirm(showId, holdId, split.seats(), split.ga())) {
            throw new HoldNoLongerValidException("Hold " + holdId + " no longer owns all of its seats");
        }
    }

    /** One sweeper batch: expired holds, locked with SKIP LOCKED so live checkouts are never waited on. */
    @Transactional
    public int expireBatch(int batchSize) {
        Instant now = clock.instant();
        List<Map.Entry<UUID, Long>> due = jdbc.sql("""
                        SELECT id, show_id FROM holds
                        WHERE status = 'HELD' AND expires_at <= :now
                        ORDER BY expires_at LIMIT :n
                        FOR UPDATE SKIP LOCKED""")
                .param("now", Sql.ts(now)).param("n", batchSize)
                .query((rs, i) -> Map.entry(rs.getObject("id", UUID.class), rs.getLong("show_id")))
                .list();
        due.forEach(h -> end(h.getKey(), h.getValue(), "EXPIRED", now));
        return due.size();
    }

    /** Moves a HELD hold to a final state and returns its inventory. False if it was not HELD. */
    private boolean end(UUID holdId, long showId, String status, Instant now) {
        int moved = jdbc.sql("UPDATE holds SET status = :st, updated_at = :now WHERE id = :id AND status = 'HELD'")
                .param("st", status).param("now", Sql.ts(now)).param("id", holdId)
                .update();
        if (moved == 0) {
            return false;
        }
        inventory.release(showId, holdId, split(holdId).ga());
        if ("EXPIRED".equals(status)) {
            expired.increment();
        }
        log.info("Hold ended hold_id={} show_id={} status={}", holdId, showId, status);
        return true;
    }

    private record ItemsSplit(int seats, List<GaItem> ga) {
    }

    private ItemsSplit split(UUID holdId) {
        int[] seats = {0};
        List<GaItem> ga = new ArrayList<>();
        jdbc.sql("SELECT venue_seat_id, tier_id, ga_shard, quantity FROM hold_items WHERE hold_id = :h")
                .param("h", holdId)
                .query((ResultSet rs) -> {
                    if (rs.getObject("venue_seat_id") != null) {
                        seats[0]++;
                    } else {
                        ga.add(new GaItem(rs.getLong("tier_id"), rs.getObject("ga_shard", Integer.class), rs.getInt("quantity")));
                    }
                });
        return new ItemsSplit(seats[0], ga);
    }

    private void insertItem(UUID holdId, long showId, long tierId, Long seatId, Integer shard, int qty, long price, String label) {
        jdbc.sql("""
                        INSERT INTO hold_items (hold_id, show_id, tier_id, venue_seat_id, ga_shard, quantity, price_minor, label)
                        VALUES (:h, :s, :t, :seat, :shard, :q, :p, :l)""")
                .param("h", holdId).param("s", showId).param("t", tierId).param("seat", seatId).param("shard", shard)
                .param("q", qty).param("p", price).param("l", label)
                .update();
    }

    private static HoldItem seatItem(ClaimedSeat s, Map<Long, Long> prices) {
        return new HoldItem(s.seatId(), s.tierId(), s.label(), 1, price(prices, s.tierId()));
    }

    private static long price(Map<Long, Long> prices, long tierId) {
        Long p = prices.get(tierId);
        if (p == null) {
            throw new IllegalStateException("No price for tier " + tierId);
        }
        return p;
    }

    private static Kind kindOf(HoldRequest r) {
        boolean seats = r.seatIds() != null && !r.seatIds().isEmpty();
        boolean ga = r.tierId() != null && r.quantity() != null;
        boolean best = r.tierId() != null && r.bestAvailable() != null;
        int forms = (seats ? 1 : 0) + (ga ? 1 : 0) + (best ? 1 : 0);
        if (forms != 1) {
            throw ApiException.badRequest("INVALID_HOLD_REQUEST",
                    "Send exactly one of: seatIds, tierId+quantity, tierId+bestAvailable");
        }
        return seats ? Kind.SEATS : ga ? Kind.GA : Kind.BEST_AVAILABLE;
    }

    private static ApiException holdLimit(int max) {
        return new ApiException(HttpStatus.CONFLICT, "HOLD_LIMIT_REACHED",
                "You already have " + max + " active hold(s) for this show", Map.of("max", max));
    }

    static ApiException holdNotFound() {
        return ApiException.notFound("HOLD_NOT_FOUND", "Hold not found");
    }

    private static void backoff(int attempt) {
        try {
            Thread.sleep(ThreadLocalRandom.current().nextLong(5, 20L * attempt + 5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
