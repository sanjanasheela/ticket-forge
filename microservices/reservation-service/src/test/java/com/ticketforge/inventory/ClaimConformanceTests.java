package com.ticketforge.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntFunction;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;

import com.ticketforge.IntegrationTest;
import com.ticketforge.catalog.ShowCategory;
import com.ticketforge.reservation.HoldViews.HoldRequest;
import com.ticketforge.reservation.HoldViews.HoldView;
import com.ticketforge.reservation.ReservationService;
import com.ticketforge.shared.ApiException;

/**
 * The contract every lock strategy must meet (spec §6): hammer the same inventory from many
 * threads released at the same instant, then check the database, not just the responses.
 */
class ClaimConformanceTests extends IntegrationTest {

    private static final int THREADS = 200;

    @Autowired
    ReservationService reservations;

    @Autowired
    InventorySettings settings;

    private LockStrategy previousStrategy;
    private boolean previousGapRule;

    @BeforeEach
    void setUp() {
        ensureUsers(THREADS);
        previousStrategy = settings.strategy();
        previousGapRule = settings.gapRuleEnabled();
        settings.gapRuleEnabled(false); // pure locking behaviour; the gap rule has its own tests
    }

    @AfterEach
    void restore() {
        settings.strategy(previousStrategy);
        settings.gapRuleEnabled(previousGapRule);
    }

    @ParameterizedTest
    @EnumSource(LockStrategy.class)
    void sameSeatsStampedeHasExactlyOneWinner(LockStrategy strategy) throws Exception {
        settings.strategy(strategy);
        TestShow show = createShow(ShowCategory.MOVIE, 1, 10, null, false, 0, 0);
        List<Long> wanted = List.of(show.seats().get(3), show.seats().get(4));

        Results r = race(i -> new HoldRequest(wanted, null, null, null), show.showId());

        assertThat(r.winners).hasSize(1);
        assertThat(r.unexpected).isEmpty();
        UUID winner = r.winners.getFirst().holdId();
        assertThat(heldBy(show.showId(), wanted)).containsOnly(winner);
    }

    @ParameterizedTest
    @EnumSource(LockStrategy.class)
    void overlappingMultiSeatClaimsNeverShareASeat(LockStrategy strategy) throws Exception {
        settings.strategy(strategy);
        TestShow show = createShow(ShowCategory.MOVIE, 1, 8, null, false, 0, 0);

        Results r = race(i -> {
            List<Long> seats = new ArrayList<>(show.seats());
            Collections.shuffle(seats, ThreadLocalRandom.current());
            return new HoldRequest(seats.subList(0, 1 + ThreadLocalRandom.current().nextInt(3)), null, null, null);
        }, show.showId());

        assertThat(r.unexpected).isEmpty();
        Map<Long, UUID> owner = new ConcurrentHashMap<>();
        for (HoldView h : r.winners) {
            h.items().forEach(item -> assertThat(owner.putIfAbsent(item.seatId(), h.holdId()))
                    .as("seat %d sold twice", item.seatId()).isNull());
        }
        // and the table agrees with the responses, seat by seat
        owner.forEach((seat, hold) -> assertThat(heldBy(show.showId(), List.of(seat))).containsOnly(hold));
    }

    @ParameterizedTest
    @EnumSource(LockStrategy.class)
    void gaCounterNeverOversells(LockStrategy strategy) throws Exception {
        settings.strategy(strategy);
        TestShow show = createShow(ShowCategory.MOVIE, 0, 0, 50, false, 0, 0);

        Results r = race(i -> new HoldRequest(null, show.gaTierId(), 1, null), show.showId());

        assertThat(r.unexpected).isEmpty();
        int held = jdbc.sql("SELECT held FROM ga_inventory WHERE show_id = :s").param("s", show.showId())
                .query(Integer.class).single();
        // Safety, for every strategy: the counter matches the winners and never exceeds capacity.
        assertThat(held).isEqualTo(r.winners.size()).isLessThanOrEqualTo(50);
        if (strategy != LockStrategy.OPTIMISTIC) {
            // Liveness: with 200 buyers for 50 tickets, everything sells.
            assertThat(r.winners).hasSize(50);
        }
        // OPTIMISTIC on one hot row is safe but not live: 200 buyers keep invalidating each other's
        // version, some exhaust their retries (CONTENTION) and tickets stay unsold despite demand.
    }

    @ParameterizedTest
    @EnumSource(LockStrategy.class)
    void bestAvailableGivesEveryWinnerDifferentSeats(LockStrategy strategy) throws Exception {
        settings.strategy(strategy);
        TestShow show = createShow(ShowCategory.MOVIE, 3, 10, null, false, 0, 0);

        Results r = race(i -> new HoldRequest(null, show.reservedTierId(), null, 2), show.showId());

        assertThat(r.unexpected).isEmpty();
        assertThat(r.winners).hasSize(15); // 30 seats / 2
        Set<Long> seen = ConcurrentHashMap.newKeySet();
        r.winners.forEach(h -> h.items().forEach(i -> assertThat(seen.add(i.seatId())).isTrue()));
    }

    @org.junit.jupiter.api.Test
    void shardedGaSellsExactlyCapacity() throws Exception {
        TestShow show = createShow(ShowCategory.MOVIE, 0, 0, 50, false, 0, 4);

        Results r = race(i -> new HoldRequest(null, show.gaTierId(), 1, null), show.showId());

        assertThat(r.unexpected).isEmpty();
        assertThat(r.winners).hasSize(50);
        assertThat(jdbc.sql("SELECT sum(held) FROM ga_inventory_shards WHERE show_id = :s").param("s", show.showId())
                .query(Integer.class).single()).isEqualTo(50);
    }

    // ---- harness -----------------------------------------------------------------------------

    private record Results(List<HoldView> winners, List<Throwable> unexpected) {
    }

    /** THREADS different users fire at once (start gate), each with its own request. */
    private Results race(IntFunction<HoldRequest> request, long showId) throws Exception {
        CountDownLatch gate = new CountDownLatch(1);
        List<Future<HoldView>> futures = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 1; i <= THREADS; i++) {
                UUID user = user(i);
                HoldRequest req = request.apply(i);
                futures.add(pool.submit(() -> {
                    gate.await();
                    return reservations.createHold(user, showId, req, null);
                }));
            }
            gate.countDown();
            List<HoldView> winners = Collections.synchronizedList(new ArrayList<>());
            List<Throwable> unexpected = Collections.synchronizedList(new ArrayList<>());
            for (Future<HoldView> f : futures) {
                try {
                    winners.add(f.get());
                } catch (java.util.concurrent.ExecutionException e) {
                    if (!isExpectedLoss(e.getCause())) {
                        unexpected.add(e.getCause());
                    }
                }
            }
            return new Results(winners, unexpected);
        }
    }

    /** Losing a race must surface as a clean business error, never as a 500. */
    private static boolean isExpectedLoss(Throwable t) {
        return t instanceof ApiException api && Set.of("SEATS_UNAVAILABLE", "GA_SOLD_OUT", "NOT_ENOUGH_SEATS", "CONTENTION")
                .contains(api.code());
    }

    private List<UUID> heldBy(long showId, List<Long> seats) {
        return jdbc.sql("SELECT hold_id FROM show_seats WHERE show_id = :s AND venue_seat_id IN (:ids)")
                .param("s", showId).param("ids", seats)
                .query(UUID.class).list();
    }
}
