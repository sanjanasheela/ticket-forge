"""Samples resource use once a second into JSONL until killed (Phase 2 version of the monolith sampler).

Per second it records:
  app_cpu_cores    CPU of the four service JVMs together          (the monolith's single "app_cpu_cores")
  infra_cpu_cores  CPU of Kong + Kafka + Redis containers         (the price of the distributed stack)
  pg_cpu_cores     CPU of the PostgreSQL container
  pool_*, heap_*   summed over the four services' /actuator/prometheus
The application tier in the monolith had 4 threads for what is app_cpu + infra_cpu here: compare the sum.

Usage: sampler.py <out.jsonl> <repo microservices dir>"""
import json, os, re, subprocess, sys, time, urllib.request

out, root = sys.argv[1], sys.argv[2]
HZ = os.sysconf("SC_CLK_TCK")
PORTS = {"auth-service": 8081, "catalog-service": 8082, "reservation-service": 8083, "booking-service": 8084}
pids = {n: int(open(f"{root}/target/{n}.pid").read()) for n in PORTS}


def cgroup_cpu_file(container):
    cid = subprocess.check_output(["docker", "inspect", "-f", "{{.Id}}", container]).decode().strip()
    return next(p for p in (f"/sys/fs/cgroup/system.slice/docker-{cid}.scope/cpu.stat", f"/sys/fs/cgroup/docker/{cid}/cpu.stat")
                if os.path.exists(p))


CONTAINERS = {c: cgroup_cpu_file(c) for c in ("tf-pg", "tf-kong", "tf-kafka", "tf-redis")}
METRICS = {
    "pool_active": r'^hikaricp_connections_active\{[^}]*\} (\S+)',
    "pool_pending": r'^hikaricp_connections_pending\{[^}]*\} (\S+)',
    "jvm_threads": r'^jvm_threads_live_threads\{[^}]*\} (\S+)',
    "heap_bytes": r'^jvm_memory_used_bytes\{[^}]*area="heap"[^}]*\} (\S+)',
    "sse_backlog": r'^ticketforge_sse_fanout_backlog\{[^}]*\} (\S+)',
    "queue_waiting": r'^ticketforge_queue_waiting\{[^}]*\} (\S+)',
    "outbox_failures": r'^ticketforge_outbox_relay_failures_total\{[^}]*\} (\S+)',
}


def jvm_ticks():
    total = 0
    for pid in pids.values():
        f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
        total += int(f[11]) + int(f[12])
    return total


def usec(container):
    return int(re.search(r"usage_usec (\d+)", open(CONTAINERS[container]).read()).group(1))


def rss_mb():
    return sum(int(re.search(r"VmRSS:\s+(\d+)", open(f"/proc/{p}/status").read()).group(1)) for p in pids.values()) / 1024


prev_t, prev_j = time.time(), jvm_ticks()
prev_c = {c: usec(c) for c in CONTAINERS}
with open(out, "w") as f:
    while True:
        time.sleep(1)
        t, j = time.time(), jvm_ticks()
        c = {k: usec(k) for k in CONTAINERS}
        dt = t - prev_t
        row = {"t": round(t, 1),
               "app_cpu_cores": round((j - prev_j) / HZ / dt, 2),
               "infra_cpu_cores": round(sum(c[k] - prev_c[k] for k in ("tf-kong", "tf-kafka", "tf-redis")) / 1e6 / dt, 2),
               "pg_cpu_cores": round((c["tf-pg"] - prev_c["tf-pg"]) / 1e6 / dt, 2),
               "app_rss_mb": round(rss_mb())}
        prev_t, prev_j, prev_c = t, j, c
        sums = {k: None for k in METRICS}
        for port in PORTS.values():
            try:
                text = urllib.request.urlopen(f"http://localhost:{port}/actuator/prometheus", timeout=2).read().decode()
            except Exception as e:
                row["scrape_error"] = f"{port}: {str(e)[:60]}"
                continue
            for k, rx in METRICS.items():
                vals = [float(v) for v in re.findall(rx, text, re.M)]
                if vals:
                    sums[k] = (sums[k] or 0) + sum(vals)
        row.update(sums)
        f.write(json.dumps(row) + "\n")
        f.flush()
