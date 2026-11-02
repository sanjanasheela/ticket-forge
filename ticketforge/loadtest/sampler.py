"""Samples app + Postgres resource use once a second into JSONL until killed.
Usage: sampler.py <out.jsonl> <postgres-container>"""
import json, os, re, subprocess, sys, time, urllib.request

out, pg = sys.argv[1], sys.argv[2]
HZ = os.sysconf("SC_CLK_TCK")
app_pid = int(subprocess.check_output(["pgrep", "-f", "^java .*ticketforge"]).split()[0])
pg_id = subprocess.check_output(["docker", "inspect", "-f", "{{.Id}}", pg]).decode().strip()
pg_cpu_file = next(p for p in (f"/sys/fs/cgroup/system.slice/docker-{pg_id}.scope/cpu.stat",
                               f"/sys/fs/cgroup/docker/{pg_id}/cpu.stat") if os.path.exists(p))
METRICS = {
    "pool_active": r'^hikaricp_connections_active\{[^}]*\} (\S+)',
    "pool_pending": r'^hikaricp_connections_pending\{[^}]*\} (\S+)',
    "jvm_threads": r'^jvm_threads_live_threads\{[^}]*\} (\S+)',
    "heap_bytes": r'^jvm_memory_used_bytes\{[^}]*area="heap"[^}]*\} (\S+)',
    "sse_backlog": r'^ticketforge_sse_fanout_backlog\{[^}]*\} (\S+)',
    "queue_waiting": r'^ticketforge_queue_waiting\{[^}]*\} (\S+)',
}


def app_ticks():
    f = open(f"/proc/{app_pid}/stat").read().rsplit(")", 1)[1].split()
    return int(f[11]) + int(f[12])


def pg_usec():
    return int(re.search(r"usage_usec (\d+)", open(pg_cpu_file).read()).group(1))


def rss_mb(pid):
    return int(re.search(r"VmRSS:\s+(\d+)", open(f"/proc/{pid}/status").read()).group(1)) / 1024


prev_t, prev_app, prev_pg = time.time(), app_ticks(), pg_usec()
with open(out, "w") as f:
    while True:
        time.sleep(1)
        t, a, p = time.time(), app_ticks(), pg_usec()
        row = {"t": round(t, 1),
               "app_cpu_cores": round((a - prev_app) / HZ / (t - prev_t), 2),
               "pg_cpu_cores": round((p - prev_pg) / 1e6 / (t - prev_t), 2),
               "app_rss_mb": round(rss_mb(app_pid))}
        prev_t, prev_app, prev_pg = t, a, p
        try:
            text = urllib.request.urlopen("http://localhost:8080/actuator/prometheus", timeout=2).read().decode()
            for k, rx in METRICS.items():
                vals = [float(v) for v in re.findall(rx, text, re.M)]
                row[k] = sum(vals) if vals else None
        except Exception as e:
            row["scrape_error"] = str(e)[:80]
        f.write(json.dumps(row) + "\n")
        f.flush()
