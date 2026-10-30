#!/usr/bin/env bash
# End-to-end check through Kong (:8080) of the whole Phase 2 path:
#   login -> create venue/event/show (catalog) -> inventory appears (Kafka) -> hold (reservation)
#   -> checkout (booking saga: charge, ConfirmHold over Kafka, book) -> booking visible.
# Needs the stack running (deploy/docker-compose.yml + scripts/run-stack.sh). Exits non-zero on the first failure.
set -euo pipefail
KONG=${KONG:-http://localhost:8080}
j() { python3 -I -c "import sys,json;d=json.load(sys.stdin);print(eval(sys.argv[1]))" "$1"; }
step() { printf '\n== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
post() { curl -sS -w '\n%{http_code}' -XPOST "$KONG$1" -H 'Content-Type: application/json' "${@:3}" -d "$2"; }

step "login as admin (auth-service via Kong)"
ADMIN=$(curl -fsS -XPOST $KONG/auth/login -H 'Content-Type: application/json' -d '{"email":"admin@ticketforge.local","password":"admin12345"}' | j 'd["accessToken"]')
A=(-H "Authorization: Bearer $ADMIN")

step "create venue, event, show (catalog-service)"
VENUE=$(curl -fsS -XPOST $KONG/admin/venues "${A[@]}" -H 'Content-Type: application/json' -d '{"name":"Smoke Hall","city":"Pune","tiers":[{"name":"Gold","kind":"RESERVED"}],"sections":[{"name":"M","rows":[{"labels":"A-B","tier":"Gold","seats":5}]}]}')
VID=$(echo "$VENUE" | j 'd["id"]'); TIER=$(echo "$VENUE" | j 'd["tiers"][0]["id"]')
EID=$(curl -fsS -XPOST $KONG/admin/events "${A[@]}" -H 'Content-Type: application/json' -d '{"title":"Smoke","genre":"Test","durationMin":90,"category":"MOVIE"}' | j 'd["id"]')
STARTS=$(date -u -d '+1 day' +%Y-%m-%dT%H:%M:%SZ)
SID=$(curl -fsS -XPOST $KONG/admin/shows "${A[@]}" -H 'Content-Type: application/json' -d "{\"eventId\":$EID,\"venueId\":$VID,\"startsAt\":\"$STARTS\",\"prices\":[{\"tierId\":$TIER,\"priceMinor\":25000}]}" | j 'd["id"]')
echo "venue=$VID event=$EID show=$SID"

step "read the show twice (second read should come from the Redis cache)"
curl -fsS $KONG/shows/$SID >/dev/null; curl -fsS $KONG/shows/$SID >/dev/null
docker exec tf-redis redis-cli -p 6380 exists "catalog:show:$SID" | grep -q 1 || fail "show not cached in Redis"
echo "cached: catalog:show:$SID"

step "wait for reservation-service to build the inventory from the Kafka event"
for i in $(seq 1 40); do
  code=$(curl -s -o /tmp/seats.json -w '%{http_code}' $KONG/shows/$SID/seats); [ "$code" = 200 ] && break; sleep 0.5
done
[ "$code" = 200 ] || fail "inventory never appeared (last status $code)"
SEATS=$(python3 -I -c "import json;d=json.load(open('/tmp/seats.json'));print(len(d['seats']), d['seats'][3]['id'], d['seats'][4]['id'])")
read -r N S4 S5 <<<"$SEATS"; echo "seats=$N (using $S4,$S5) after ~$((i/2))s"
[ "$N" = 10 ] || fail "expected 10 seats"

step "signup a buyer (auth-service) and hold two seats (reservation-service)"
SIGN=$(curl -fsS -XPOST $KONG/auth/signup -H 'Content-Type: application/json' -d "{\"email\":\"smoke-$RANDOM$RANDOM@test.local\",\"password\":\"password123\"}")
TOKEN=$(echo "$SIGN" | j 'd["accessToken"]'); B=(-H "Authorization: Bearer $TOKEN")
HOLD=$(curl -fsS -XPOST $KONG/shows/$SID/holds "${B[@]}" -H 'Content-Type: application/json' -d "{\"seatIds\":[$S4,$S5]}")
HID=$(echo "$HOLD" | j 'd["holdId"]'); echo "hold=$HID total=$(echo "$HOLD" | j 'd["totalMinor"]')"

step "checkout (booking-service saga: charge -> ConfirmHold over Kafka -> book)"
for try in 1 2 3 4 5; do
  OUT=$(curl -sS -w '\n%{http_code}' -XPOST $KONG/holds/$HID/checkout "${B[@]}" -H "Idempotency-Key: smoke-$RANDOM$RANDOM")
  CODE=${OUT##*$'\n'}; BODY=${OUT%$'\n'*}
  [ "$CODE" = 201 ] && break; [ "$CODE" = 402 ] && { echo "(mock gateway declined, retrying)"; continue; }; break
done
echo "HTTP $CODE $BODY"
[ "$CODE" = 201 ] || fail "checkout did not confirm"

step "booking visible, seats booked, hold confirmed"
curl -fsS $KONG/me/bookings "${B[@]}" | j '"bookings=%d items=%d" % (len(d), len(d[0]["items"]))'
curl -fsS $KONG/shows/$SID/seats | python3 -I -c "import sys,json;d=json.load(sys.stdin);print('booked seats:',[s['id'] for s in d['seats'] if s['st']=='B'])"
curl -fsS $KONG/holds/$HID "${B[@]}" | j '"hold status=" + d["status"]'
step "SMOKE TEST PASSED"
