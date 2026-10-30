package com.ticketforge.projection;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.ticketforge.IntegrationTest;
import com.ticketforge.catalog.ShowCategory;
import com.ticketforge.contracts.ShowSnapshot;
import com.ticketforge.contracts.VenueLayout;

/** The catalog-to-inventory projection: what a snapshot and a layout turn into, and what repeating them does. */
class ShowProjectionTests extends IntegrationTest {

    private static ShowSnapshot snapshot(long showId, int gaShards, long price) {
        Instant now = Instant.now();
        return new ShowSnapshot(showId, showId, "MOVIE", now.plus(Duration.ofDays(1)), now.minusSeconds(60), 420, false, 10,
                null, 200, gaShards, "INR", "ACTIVE", List.of(new ShowSnapshot.TierPrice(1, price), new ShowSnapshot.TierPrice(2, 500)));
    }

    private static VenueLayout layout(long venueId, int seats, Integer gaCapacity) {
        List<VenueLayout.Seat> list = new java.util.ArrayList<>();
        for (int i = 1; i <= seats; i++) {
            list.add(new VenueLayout.Seat(i, 1, "MAIN", "A", 0, i, false, false));
        }
        return new VenueLayout(venueId, list, gaCapacity == null ? List.of() : List.of(new VenueLayout.GaTier(2, gaCapacity)));
    }

    private int seatCount(long showId) {
        return jdbc.sql("SELECT count(*) FROM show_seats WHERE show_id = :s").param("s", showId).query(Integer.class).single();
    }

    @Test
    void aShowIsNotBookableUntilItsInventoryIsBuilt() {
        long id = 7_700_001;
        projection.upsertShow(snapshot(id, 0, 1000));

        assertThat(projection.inventoryReady(id)).isFalse();
        assertThat(get("/shows/" + id + "/seats", null)).as("rules known, no seats yet").hasStatus(404);

        projection.materialiseInventory(snapshot(id, 0, 1000), layout(id, 12, null));

        assertThat(projection.inventoryReady(id)).isTrue();
        assertThat(seatCount(id)).isEqualTo(12);
        assertThat(get("/shows/" + id + "/seats", null)).hasStatusOk();
    }

    @Test
    void applyingTheSameSnapshotAndLayoutTwiceChangesNothing() {
        long id = 7_700_002;
        projection.apply(snapshot(id, 0, 1000), layout(id, 8, 40));
        projection.apply(snapshot(id, 0, 1000), layout(id, 8, 40));

        assertThat(seatCount(id)).isEqualTo(8);
        assertThat(jdbc.sql("SELECT count(*) FROM ga_inventory WHERE show_id = :s").param("s", id).query(Integer.class).single()).isEqualTo(1);
        assertThat(jdbc.sql("SELECT count(*) FROM show_tier_prices WHERE show_id = :s").param("s", id).query(Integer.class).single()).isEqualTo(2);
    }

    @Test
    void aPriceChangeUpdatesPricesButNeverRebuildsOrUnreadiesInventory() {
        long id = 7_700_003;
        projection.apply(snapshot(id, 0, 1000), layout(id, 5, null));
        jdbc.sql("UPDATE show_seats SET status = 'HELD', hold_id = gen_random_uuid(), held_until = now() + interval '5 min' WHERE show_id = :s AND venue_seat_id = 1")
                .param("s", id).update();

        projection.upsertShow(snapshot(id, 0, 9999));          // ShowPublished with a new price
        projection.materialiseInventory(snapshot(id, 0, 9999), layout(id, 5, null));   // a repeat must be a no-op

        assertThat(jdbc.sql("SELECT price_minor FROM show_tier_prices WHERE show_id = :s AND tier_id = 1").param("s", id).query(Long.class).single()).isEqualTo(9999);
        assertThat(projection.inventoryReady(id)).isTrue();
        assertThat(jdbc.sql("SELECT status FROM show_seats WHERE show_id = :s AND venue_seat_id = 1").param("s", id).query(String.class).single())
                .as("a held seat stays held").isEqualTo("HELD");
        jdbc.sql("UPDATE show_seats SET status = 'AVAILABLE', hold_id = NULL, held_until = NULL WHERE show_id = :s").param("s", id).update();
    }

    @Test
    void gaShardsSplitTheCapacityWithTheRemainderOnTheFirstShards() {
        long id = 7_700_004;
        projection.apply(snapshot(id, 4, 1000), layout(id, 0, 10));   // 10 over 4 shards: 3, 3, 2, 2

        List<Integer> capacities = jdbc.sql("SELECT capacity FROM ga_inventory_shards WHERE show_id = :s ORDER BY shard")
                .param("s", id).query(Integer.class).list();
        assertThat(capacities).containsExactly(3, 3, 2, 2);
    }

    @Test
    void aBigVenueMaterialisesInOneStatement() {
        long id = 7_700_005;
        long start = System.nanoTime();
        projection.apply(snapshot(id, 0, 1000), layout(id, 50_000, null));
        long ms = Duration.ofNanos(System.nanoTime() - start).toMillis();

        assertThat(seatCount(id)).isEqualTo(50_000);
        assertThat(ms).as("50k seats should take about a second, not minutes").isLessThan(15_000);
    }
}
