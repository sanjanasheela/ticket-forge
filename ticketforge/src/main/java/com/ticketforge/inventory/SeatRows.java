package com.ticketforge.inventory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

import com.ticketforge.shared.Sql;

/** Reads seats for claims. Every locking read orders by seat id, so lock order is the same everywhere. */
@Component
class SeatRows {

    static final String COLUMNS = """
            venue_seat_id, tier_id, section, row_label, seat_number, aisle_left, aisle_right, status, held_until, version""";

    private final JdbcClient jdbc;

    SeatRows(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    /** The requested seats only, optionally locked. */
    List<RowSeat> seats(long showId, Collection<Long> seatIds, boolean lock) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM show_seats WHERE show_id = :s AND venue_seat_id IN (:ids)"
                        + " ORDER BY venue_seat_id" + (lock ? " FOR UPDATE" : ""))
                .param("s", showId)
                .param("ids", seatIds)
                .query(SeatRows::map)
                .list();
    }

    /**
     * Every seat in the rows touched by the request (what the gap rule needs), ordered for the rule.
     * With {@code lock}, the whole rows are locked (in seat-id order) so no concurrent claim can
     * change a neighbour: that is what makes PESSIMISTIC gap-race free, at the cost of contention.
     * <p>
     * Two steps on purpose: find the rows by primary key, then fetch them by exact
     * (show_id, section, row_label) matches. The one-statement version (a semi-join) was planned as
     * a nested loop over the whole show whenever statistics predated the show's 50k fresh rows.
     */
    List<RowSeat> rowsOf(long showId, Collection<Long> seatIds, boolean lock) {
        List<String[]> rows = jdbc.sql("SELECT DISTINCT section, row_label FROM show_seats WHERE show_id = :s AND venue_seat_id IN (:ids)")
                .param("s", showId).param("ids", seatIds)
                .query((rs, i) -> new String[] {rs.getString(1), rs.getString(2)})
                .list();
        if (rows.isEmpty()) {
            return List.of();
        }
        StringBuilder where = new StringBuilder();
        for (int i = 0; i < rows.size(); i++) {
            where.append(i == 0 ? "" : " OR ").append("(section = :sec").append(i).append(" AND row_label = :row").append(i).append(')');
        }
        var stmt = jdbc.sql("SELECT " + COLUMNS + " FROM show_seats WHERE show_id = :s AND (" + where + ")"
                + " ORDER BY venue_seat_id" + (lock ? " FOR UPDATE" : ""));
        stmt = stmt.param("s", showId);
        for (int i = 0; i < rows.size(); i++) {
            stmt = stmt.param("sec" + i, rows.get(i)[0]).param("row" + i, rows.get(i)[1]);
        }
        List<RowSeat> seats = new java.util.ArrayList<>(stmt.query(SeatRows::map).list());
        seats.sort(RULE_ORDER);
        return seats;
    }

    private static final java.util.Comparator<RowSeat> RULE_ORDER = java.util.Comparator
            .comparing(RowSeat::section).thenComparing(RowSeat::row).thenComparingInt(RowSeat::number);

    static RowSeat map(ResultSet rs, int i) throws SQLException {
        return new RowSeat(rs.getLong("venue_seat_id"), rs.getLong("tier_id"), rs.getString("section"),
                rs.getString("row_label"), rs.getInt("seat_number"), rs.getBoolean("aisle_left"),
                rs.getBoolean("aisle_right"), rs.getString("status"), Sql.instant(rs.getTimestamp("held_until")),
                rs.getLong("version"));
    }
}
