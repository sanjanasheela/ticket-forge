-- Spec §4 invariants. Every row must show 0. Run after a load test:
--   docker exec -i <postgres> psql -U ticketforge ticketforge < scripts/verify-invariants.sql
-- Generated from InvariantChecker.java; keep them in sync.

SELECT 'seat_sold_twice' AS invariant, (SELECT count(*) FROM (
    SELECT hi.show_id, hi.venue_seat_id FROM hold_items hi JOIN holds h ON h.id = hi.hold_id
    WHERE h.status = 'CONFIRMED' AND hi.venue_seat_id IS NOT NULL
    GROUP BY 1, 2 HAVING count(*) > 1) x) AS violations;

SELECT 'booked_seat_without_confirmed_hold' AS invariant, (SELECT count(*) FROM show_seats s LEFT JOIN holds h ON h.id = s.hold_id
WHERE s.status = 'BOOKED' AND (h.id IS NULL OR h.status <> 'CONFIRMED')) AS violations;

SELECT 'confirmed_seat_not_booked' AS invariant, (SELECT count(*) FROM hold_items hi JOIN holds h ON h.id = hi.hold_id
LEFT JOIN show_seats s ON s.show_id = hi.show_id AND s.venue_seat_id = hi.venue_seat_id
WHERE h.status = 'CONFIRMED' AND hi.venue_seat_id IS NOT NULL
  AND (s.status IS DISTINCT FROM 'BOOKED' OR s.hold_id IS DISTINCT FROM h.id)) AS violations;

SELECT 'ga_counter_mismatch' AS invariant, (WITH items AS (
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
WHERE c.held <> COALESCE(i.held, 0) OR c.sold <> COALESCE(i.sold, 0)) AS violations;

SELECT 'paid_without_booking' AS invariant, (SELECT count(*) FROM payments p LEFT JOIN bookings b ON b.payment_id = p.id
WHERE p.status = 'SUCCEEDED' AND b.id IS NULL) AS violations;

SELECT 'booking_without_payment' AS invariant, (SELECT count(*) FROM bookings b JOIN payments p ON p.id = b.payment_id
WHERE b.status = 'CONFIRMED' AND p.status <> 'SUCCEEDED') AS violations;
