package com.ticketforge.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

class GapRuleTests {

    /** One row of {@code n} seats with ids 1..n; {@code aisleAfter} seats have an aisle on their right. */
    private static List<RowSeat> row(int n, int... aisleAfter) {
        Set<Integer> aisles = new java.util.HashSet<>();
        for (int a : aisleAfter) {
            aisles.add(a);
        }
        List<RowSeat> seats = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            seats.add(new RowSeat(i, 1, "MAIN", "A", i, aisles.contains(i - 1), aisles.contains(i), "AVAILABLE", null, 0));
        }
        return seats;
    }

    private static Set<Long> newOrphans(List<RowSeat> seats, Set<Long> occupied, List<Long> selected) {
        return GapRule.newOrphans(seats, selected, s -> occupied.contains(s.seatId()));
    }

    @Test
    void middleSeatsInAnEmptyRowAreFine() {
        assertThat(newOrphans(row(10), Set.of(), List.of(4L, 5L))).isEmpty();
    }

    @Test
    void leavingOneSeatAtTheRowEdgeIsRejected() {
        assertThat(newOrphans(row(10), Set.of(), List.of(2L, 3L))).containsExactly(1L);
    }

    @Test
    void leavingOneSeatBetweenTwoTakenSeatsIsRejected() {
        assertThat(newOrphans(row(10), Set.of(5L), List.of(3L))).containsExactly(4L);
    }

    @Test
    void fillingAnExistingGapIsAllowed() {
        assertThat(newOrphans(row(10), Set.of(3L, 5L), List.of(4L))).isEmpty();
    }

    @Test
    void preExistingOrphansDoNotBlockOtherSelections() {
        // seat 2 is already an orphan between 1 and 3; picking 7-8 elsewhere must still work
        assertThat(newOrphans(row(10), Set.of(1L, 3L), List.of(7L, 8L))).isEmpty();
    }

    @Test
    void aislesSplitTheRowIntoSegments() {
        // aisle after seat 5: taking 4 leaves seat 5 alone against the aisle
        assertThat(newOrphans(row(10, 5), Set.of(), List.of(4L))).containsExactly(5L);
        // taking 4-5 is fine, the aisle is a natural edge
        assertThat(newOrphans(row(10, 5), Set.of(), List.of(4L, 5L))).isEmpty();
    }

    @Test
    void aSingleSeatSegmentIsNeverAnOrphan() {
        assertThat(newOrphans(row(3, 1), Set.of(), List.of(2L, 3L))).isEmpty();
    }
}
