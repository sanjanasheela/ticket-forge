#!/usr/bin/env python3
"""Checks every Phase 1 invariant against the four Phase 2 databases, plus the two that only exist across services.
Prints one JSON object; every value must be 0. Run after any load or failure test.

It reads the databases directly through `docker exec psql`. That is deliberate and allowed here: it is a test harness
standing outside the system, the one place that may look at all four databases at once.

Cross-service checks tolerate in-flight work: only holds/bookings older than SETTLE seconds count, because a checkout
is eventually consistent while its saga runs (see docs/PHASE2_DESIGN.md section 5)."""
import json, subprocess, sys

SETTLE = int(sys.argv[1]) if len(sys.argv) > 1 else 30
PG = "tf-pg"


def q(db, sql):
    return subprocess.check_output(["docker", "exec", PG, "psql", "-U", "ticketforge", "-d", db, "-At", "-c", sql]).decode().strip()


def one(db, sql):
    return int(q(db, sql))


r = {}
# reservation_db: seat and GA-counter integrity (the same SQL as Phase 1)
r["seat_sold_twice"] = one("reservation_db", """SELECT count(*) FROM (SELECT hi.show_id, hi.venue_seat_id FROM hold_items hi
  JOIN holds h ON h.id = hi.hold_id WHERE h.status = 'CONFIRMED' AND hi.venue_seat_id IS NOT NULL
  GROUP BY 1, 2 HAVING count(*) > 1) x""")
r["booked_seat_without_confirmed_hold"] = one("reservation_db", """SELECT count(*) FROM show_seats s LEFT JOIN holds h ON h.id = s.hold_id
  WHERE s.status = 'BOOKED' AND (h.id IS NULL OR h.status <> 'CONFIRMED')""")
r["confirmed_seat_not_booked"] = one("reservation_db", """SELECT count(*) FROM hold_items hi JOIN holds h ON h.id = hi.hold_id
  LEFT JOIN show_seats s ON s.show_id = hi.show_id AND s.venue_seat_id = hi.venue_seat_id
  WHERE h.status = 'CONFIRMED' AND hi.venue_seat_id IS NOT NULL AND (s.status IS DISTINCT FROM 'BOOKED' OR s.hold_id IS DISTINCT FROM h.id)""")
r["ga_counter_mismatch"] = one("reservation_db", """WITH items AS (SELECT hi.show_id, hi.tier_id, COALESCE(hi.ga_shard, -1) AS shard,
    sum(hi.quantity) FILTER (WHERE h.status = 'HELD') AS held, sum(hi.quantity) FILTER (WHERE h.status = 'CONFIRMED') AS sold
  FROM hold_items hi JOIN holds h ON h.id = hi.hold_id WHERE hi.venue_seat_id IS NULL GROUP BY 1, 2, 3),
  counters AS (SELECT g.show_id, g.tier_id, -1 AS shard, g.held, g.sold FROM ga_inventory g JOIN shows s ON s.id = g.show_id WHERE s.ga_shards = 0
    UNION ALL SELECT show_id, tier_id, shard, held, sold FROM ga_inventory_shards)
  SELECT count(*) FROM counters c LEFT JOIN items i ON i.show_id = c.show_id AND i.tier_id = c.tier_id AND i.shard = c.shard
  WHERE c.held <> COALESCE(i.held, 0) OR c.sold <> COALESCE(i.sold, 0)""")
# booking_db: money
r["paid_without_booking"] = one("booking_db", f"""SELECT count(*) FROM payments p LEFT JOIN bookings b ON b.payment_id = p.id
  WHERE p.status = 'SUCCEEDED' AND b.id IS NULL
    AND NOT EXISTS (SELECT 1 FROM sagas s WHERE s.payment_id = p.id AND s.state IN ('CHARGED', 'REFUNDING'))""")
r["booking_without_payment"] = one("booking_db", """SELECT count(*) FROM bookings b JOIN payments p ON p.id = b.payment_id
  WHERE b.status = 'CONFIRMED' AND p.status <> 'SUCCEEDED'""")
# across the two: a confirmed hold must have a booking, and a booking must have a confirmed hold
confirmed = set(q("reservation_db", f"SELECT id FROM holds WHERE status = 'CONFIRMED' AND updated_at < now() - interval '{SETTLE} seconds'").split())
booked = set(q("booking_db", f"SELECT hold_id FROM bookings WHERE status = 'CONFIRMED' AND created_at < now() - interval '{SETTLE} seconds'").split())
all_confirmed = set(q("reservation_db", "SELECT id FROM holds WHERE status = 'CONFIRMED'").split())
all_booked = set(q("booking_db", "SELECT hold_id FROM bookings WHERE status = 'CONFIRMED'").split())
r["confirmed_hold_without_booking"] = len(confirmed - all_booked)
r["booking_without_confirmed_hold"] = len(booked - all_confirmed)
r["sagas_stuck_in_charged_or_refunding"] = one("booking_db", f"SELECT count(*) FROM sagas WHERE state IN ('CHARGED','REFUNDING') AND updated_at < now() - interval '{SETTLE} seconds'")
print(json.dumps(r))
sys.exit(0 if all(v == 0 for v in r.values()) else 1)
