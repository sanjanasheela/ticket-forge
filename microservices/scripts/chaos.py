#!/usr/bin/env python3
"""Failure experiments for the Phase 2 stack.

Drives steady buyer traffic through Kong (browse a show, hold a seat, check out), injects ONE fault part-way,
heals it, waits for the system to settle, then checks that nothing was lost or double-sold.

    scripts/chaos.py <experiment> [--seconds 40] [--workers 24]

    experiments: baseline | redis | catalog | reservation | booking | kafka | postgres | booking-kill9

What is measured, per phase (before the fault / during it / after healing):
  reads      GET /shows/{id}                 (catalog-service, Redis cache)
  holds      POST /shows/{id}/holds          (reservation-service)
  checkouts  POST /holds/{id}/checkout       (booking-service saga)
and, once everything has settled, the nine invariants from verify-invariants.py plus two money checks:
every successful payment has a booking or a refund, and no hold is paid twice.

Stdlib only. JWTs are minted locally with the dev secret, as the k6 scripts do.
"""
import argparse, base64, glob, hashlib, hmac, json, os, random, subprocess, sys, threading, time, urllib.error, urllib.request, uuid

KONG = os.environ.get("KONG", "http://localhost:8080")
SECRET = os.environ.get("JWT_SECRET", "dev-only-secret-change-me-0123456789abcdef").encode()
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


# ---- tiny helpers ----------------------------------------------------------------------------------------------

def b64(b):
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def jwt(sub, roles=("USER",)):
    now = int(time.time())
    head = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    body = b64(json.dumps({"iss": "ticketforge", "sub": sub, "iat": now, "exp": now + 3600, "typ": "access", "roles": list(roles)}).encode())
    sig = b64(hmac.new(SECRET, f"{head}.{body}".encode(), hashlib.sha256).digest())
    return f"{head}.{body}.{sig}"


def user_id(i):
    return "00000000-0000-4000-8000-%012d" % i


def call(method, path, token=None, body=None, headers=None, timeout=15):
    """Returns (status, parsed-json-or-None). Status 0 means no HTTP answer at all (connection refused / timeout)."""
    req = urllib.request.Request(KONG + path, method=method, data=None if body is None else json.dumps(body).encode())
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw) if raw else None
        except ValueError:
            return e.code, None
    except Exception:
        return 0, None


def sh(*cmd, check=True):
    return subprocess.run(cmd, check=check, capture_output=True, text=True).stdout.strip()


# ---- faults ----------------------------------------------------------------------------------------------------

def pid_of(service):
    return int(open(f"{ROOT}/target/{service}.pid").read())


def wait_exit(pid, seconds=90):
    """A SIGTERM'd Spring Boot service finishes in-flight requests first; starting its replacement earlier fails on the port."""
    end = time.time() + seconds
    while time.time() < end:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.5)


def service_cmd(service, port, heap):
    jar = sorted(glob.glob(f"{ROOT}/{service}/target/{service}-*-SNAPSHOT.jar"))[0]   # glob, not a shell: ROOT contains a space
    env = dict(os.environ, PORT=str(port), REDIS_PORT="6380", LOG_FORMAT="ecs")
    log = open(f"{ROOT}/target/stack-logs/{service}.log", "a")
    p = subprocess.Popen(["taskset", "-c", "0,1,6,7", "java", f"-Xmx{heap}", "-jar", jar], env=env, stdout=log, stderr=log)
    open(f"{ROOT}/target/{service}.pid", "w").write(str(p.pid))


SERVICES = {"catalog": ("catalog-service", 8082, "384m"), "reservation": ("reservation-service", 8083, "768m"),
            "booking": ("booking-service", 8084, "256m")}


def wait_healthy(port, seconds=120):
    end = time.time() + seconds
    while time.time() < end:
        try:
            urllib.request.urlopen(f"http://localhost:{port}/actuator/health", timeout=2)
            return
        except Exception:
            time.sleep(1)
    raise SystemExit(f"service on :{port} did not come back")


FAULTS = {
    "redis":       (lambda: sh("docker", "stop", "tf-redis"),           lambda: sh("docker", "start", "tf-redis"), "Redis stopped"),
    "kafka":       (lambda: sh("docker", "stop", "tf-kafka"),           lambda: sh("docker", "start", "tf-kafka"), "Kafka broker stopped"),
    "postgres":    (lambda: sh("docker", "stop", "tf-pg"),              lambda: sh("docker", "start", "tf-pg"), "PostgreSQL stopped"),
}
for key, (svc, port, heap) in SERVICES.items():
    FAULTS[key] = (lambda s=svc: os.kill(pid_of(s), 15), lambda s=svc, p=port, h=heap: (wait_exit(pid_of(s)), service_cmd(s, p, h), wait_healthy(p)), f"{svc} stopped (SIGTERM)")
FAULTS["booking-kill9"] = (lambda: os.kill(pid_of("booking-service"), 9),
                           lambda: (service_cmd("booking-service", 8084, "256m"), wait_healthy(8084)),
                           "booking-service killed with SIGKILL (no graceful shutdown)")


# ---- setup -----------------------------------------------------------------------------------------------------

def login_admin():
    s, b = call("POST", "/auth/login", body={"email": "admin@ticketforge.local", "password": "admin12345"})
    assert s == 200, f"admin login failed: {s}"
    return b["accessToken"]


def make_show(admin, rows=26, per_row=400):
    s, v = call("POST", "/admin/venues", admin, {"name": f"Chaos {time.time()}", "city": "X", "tiers": [{"name": "Gold", "kind": "RESERVED"}],
                                                  "sections": [{"name": "M", "rows": [{"labels": f"A-{chr(ord('A') + rows - 1)}", "tier": "Gold", "seats": per_row}]}]})
    assert s == 201, f"venue: {s}"
    s, e = call("POST", "/admin/events", admin, {"title": "Chaos", "genre": "X", "durationMin": 90, "category": "MOVIE"})
    starts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 86400))
    s, sh_ = call("POST", "/admin/shows", admin, {"eventId": e["id"], "venueId": v["id"], "startsAt": starts, "maxSeatsPerHold": 10,
                                                   "prices": [{"tierId": v["tiers"][0]["id"], "priceMinor": 25000}]})
    assert s == 201, f"show: {s}"
    show = sh_["id"]
    for _ in range(120):
        s, seats = call("GET", f"/shows/{show}/seats")
        if s == 200:
            return show, [x["id"] for x in seats["seats"]]
        time.sleep(0.5)
    raise SystemExit("inventory never appeared")


# ---- traffic ---------------------------------------------------------------------------------------------------

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.rows = []   # (t, kind, status, latency)

        self.samples = {}   # first response body seen for each (kind, status): what a failure actually said

    def add(self, kind, status, latency, body=None):
        with self.lock:
            self.rows.append((time.time(), kind, status, latency))
            if status not in (200, 201) and (kind, status) not in self.samples:
                self.samples[(kind, status)] = body


def worker(idx, show, seats, stats, stop, counter):
    rnd = random.Random(idx)
    token = jwt(user_id(idx + 1))
    while not stop.is_set():
        t = time.time(); s, body = call("GET", f"/shows/{show}"); stats.add("read", s, time.time() - t, body)
        with counter["lock"]:
            if not counter["free"]:
                time.sleep(0.2); continue
            seat = counter["free"].pop(rnd.randrange(len(counter["free"])))
        t = time.time(); s, hold = call("POST", f"/shows/{show}/holds", token, {"seatIds": [seat]}, {"Idempotency-Key": "h-" + str(uuid.uuid4())})
        stats.add("hold", s, time.time() - t, hold)
        if s != 201:
            with counter["lock"]:
                if s in (409,):
                    pass   # someone else's seat now: not ours to reuse
                else:
                    counter["free"].append(seat)
            continue
        time.sleep(rnd.uniform(0.05, 0.2))
        t = time.time()
        s, body = call("POST", f"/holds/{hold['holdId']}/checkout", token, headers={"Idempotency-Key": f"c-{hold['holdId']}"}, timeout=20)
        stats.add("checkout", s, time.time() - t, body)


def summarise(rows, t_fault, t_heal):
    def phase(t):
        return "before" if t < t_fault else "during" if t < t_heal else "after"
    out = {}
    for kind in ("read", "hold", "checkout"):
        for ph in ("before", "during", "after"):
            sel = [r for r in rows if r[1] == kind and phase(r[0]) == ph]
            codes = {}
            for r in sel:
                codes[r[2]] = codes.get(r[2], 0) + 1
            lat = sorted(r[3] for r in sel)
            out[(kind, ph)] = (len(sel), codes, round(lat[int(len(lat) * 0.99) - 1] * 1000) if len(lat) > 1 else None,
                               round(lat[len(lat) // 2] * 1000) if lat else None)
    return out


def money_checks():
    q = lambda sql: int(sh("docker", "exec", "tf-pg", "psql", "-U", "ticketforge", "-d", "booking_db", "-At", "-c", sql))
    return {
        "payments_succeeded_without_booking_or_refund_inflight": q("""SELECT count(*) FROM payments p WHERE p.status = 'SUCCEEDED'
            AND NOT EXISTS (SELECT 1 FROM bookings b WHERE b.payment_id = p.id)"""),
        "holds_paid_more_than_once": q("SELECT count(*) FROM (SELECT hold_id FROM payments WHERE status = 'SUCCEEDED' GROUP BY hold_id HAVING count(*) > 1) x"),
        "sagas_not_terminal": q("SELECT count(*) FROM sagas WHERE state NOT IN ('BOOKED','REFUNDED','FAILED')"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("experiment", choices=["baseline"] + list(FAULTS))
    ap.add_argument("--seconds", type=int, default=40)
    ap.add_argument("--workers", type=int, default=24)
    a = ap.parse_args()

    admin = login_admin()
    call("PUT", "/admin/payment-gateway/config", admin, {"latencyMs": 20, "jitterMs": 0, "failureRate": 0, "timeoutRate": 0})
    # Buyers here pick random single seats, which the gap rule would mostly reject; the k6 scripts turn it off too.
    call("PUT", "/admin/inventory/config", admin, {"strategy": "ATOMIC_CONDITIONAL", "gapRuleEnabled": False})
    show, seats = make_show(admin)
    print(f"experiment={a.experiment} show={show} seats={len(seats)} workers={a.workers}")
    counter = {"lock": threading.Lock(), "free": list(seats)}
    stats, stop = Stats(), threading.Event()
    threads = [threading.Thread(target=worker, args=(i, show, seats, stats, stop, counter), daemon=True) for i in range(a.workers)]
    t0 = time.time()
    for t in threads:
        t.start()

    third = a.seconds / 3
    time.sleep(third)
    t_fault = time.time()
    if a.experiment != "baseline":
        print(f"[{t_fault - t0:5.1f}s] FAULT: {FAULTS[a.experiment][2]}")
        FAULTS[a.experiment][0]()
    time.sleep(third)
    t_heal = time.time()
    if a.experiment != "baseline":
        print(f"[{t_heal - t0:5.1f}s] HEAL")
        FAULTS[a.experiment][1]()
    time.sleep(third)
    stop.set()
    for t in threads:
        t.join(timeout=30)

    print("\nphase results: count, p50 / p99 ms, status codes")
    res = summarise(stats.rows, t_fault, t_heal)
    for kind in ("read", "hold", "checkout"):
        for ph in ("before", "during", "after"):
            n, codes, p99, p50 = res[(kind, ph)]
            print(f"  {kind:9s} {ph:7s} n={n:5d} p50={str(p50):>5s} p99={str(p99):>6s}ms  {dict(sorted(codes.items()))}")

    for (kind, status), body in sorted(stats.samples.items(), key=str):
        print(f"  sample {kind} {status}: {str(body)[:160]}")
    print("\nsettling (sagas finish, outbox drains, reconciler runs) ...")
    for _ in range(40):
        time.sleep(3)
        try:
            if money_checks()["sagas_not_terminal"] == 0:
                break
        except Exception:
            pass
    time.sleep(6)
    inv = json.loads(subprocess.run(["python3", "-I", f"{HERE}/verify-invariants.py", "5"], capture_output=True, text=True).stdout)
    money = money_checks()
    print("invariants:", inv)
    print("money     :", money)
    ok = all(v == 0 for v in inv.values()) and money["holds_paid_more_than_once"] == 0 and money["sagas_not_terminal"] == 0 \
        and money["payments_succeeded_without_booking_or_refund_inflight"] == 0
    json.dump({"experiment": a.experiment, "phases": {f"{k[0]}/{k[1]}": v for k, v in res.items()}, "invariants": inv, "money": money, "ok": ok},
              open(f"{ROOT}/loadtest/results/chaos-{a.experiment}.json", "w"), indent=1, default=str)
    print("RESULT:", "PASS (no data loss, no double sale, no double charge)" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
