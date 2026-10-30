package com.ticketforge.inventory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * "No single-seat gaps" (BookMyShow style). A row is split into segments at aisles and at breaks
 * in seat numbering. A free seat is an orphan when both of its sides are blocked (an occupied seat,
 * or the segment edge) and at least one side is an occupied seat. A selection is rejected if it
 * creates an orphan that did not already exist.
 */
final class GapRule {

    private GapRule() {
    }

    /** Seats that would newly become orphans if {@code selected} were taken. Empty means allowed. */
    static Set<Long> newOrphans(List<RowSeat> seats, Collection<Long> selected, Predicate<RowSeat> occupied) {
        Set<Long> chosen = new HashSet<>(selected);
        Set<Long> before = orphans(seats, occupied);
        Set<Long> after = orphans(seats, s -> occupied.test(s) || chosen.contains(s.seatId()));
        after.removeAll(before);
        return after;
    }

    /** {@code seats} must be ordered by section, row, seat number. */
    static Set<Long> orphans(List<RowSeat> seats, Predicate<RowSeat> occupied) {
        Set<Long> result = new LinkedHashSet<>();
        for (List<RowSeat> segment : segments(seats)) {
            int m = segment.size();
            for (int i = 0; i < m; i++) {
                if (occupied.test(segment.get(i))) {
                    continue;
                }
                boolean leftOccupied = i > 0 && occupied.test(segment.get(i - 1));
                boolean rightOccupied = i < m - 1 && occupied.test(segment.get(i + 1));
                boolean leftBlocked = i == 0 || leftOccupied;
                boolean rightBlocked = i == m - 1 || rightOccupied;
                if (leftBlocked && rightBlocked && (leftOccupied || rightOccupied)) {
                    result.add(segment.get(i).seatId());
                }
            }
        }
        return result;
    }

    static List<List<RowSeat>> segments(List<RowSeat> seats) {
        List<List<RowSeat>> segments = new ArrayList<>();
        List<RowSeat> current = new ArrayList<>();
        RowSeat prev = null;
        for (RowSeat s : seats) {
            boolean breaks = prev == null
                    || !prev.section().equals(s.section())
                    || !prev.row().equals(s.row())
                    || prev.aisleRight()
                    || s.aisleLeft()
                    || s.number() != prev.number() + 1;
            if (breaks && !current.isEmpty()) {
                segments.add(current);
                current = new ArrayList<>();
            }
            current.add(s);
            prev = s;
        }
        if (!current.isEmpty()) {
            segments.add(current);
        }
        return segments;
    }
}
