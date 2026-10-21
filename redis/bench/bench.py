#!/usr/bin/env python3
"""ForgeKV (Java) vs Redis benchmark suite (milestone M10).

Starts each server, drives it with the stock `redis-benchmark`, and writes a
Markdown report plus the raw numbers:

    bench/bench.py            # full run (~5 minutes)
    bench/bench.py --quick    # smoke run (~1 minute)

Servers and client are pinned to disjoint CPUs (see --server-cpus /
--client-cpus) so they don't steal cycles from each other. Durability runs
write to bench/data (a real disk; /tmp is often tmpfs, where fsync is free).

Requires: redis-server, redis-benchmark, taskset, java 22+, mvn.
"""

import argparse
import csv
import io
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FORGEKV = ROOT / "target" / "forgekv.jar"
JAVA = ["java", "--enable-native-access=ALL-UNNAMED", "-jar"]
DATA = ROOT / "bench" / "data"

# Redis needs Lua for what ForgeKV does natively with DECRIF.
DECRIF_LUA = (
    "local v = tonumber(redis.call('GET', KEYS[1]) or '0') "
    "local n = tonumber(ARGV[2] or '1') "
    "if v - n < tonumber(ARGV[1]) then return false end "
    "return redis.call('DECRBY', KEYS[1], n)"
)


# --------------------------------------------------------------------------
# tiny RESP client (setup only; avoids a redis-py dependency)
# --------------------------------------------------------------------------

def resp(*parts):
    out = [f"*{len(parts)}\r\n".encode()]
    for p in parts:
        b = p if isinstance(p, bytes) else str(p).encode()
        out.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
    return b"".join(out)


def command(port, *parts, timeout=5.0):
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        s.sendall(resp(*parts))
        return s.recv(1 << 16)


def wait_ready(port, proc, timeout=30.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server on :{port} exited with {proc.returncode}")
        try:
            if command(port, "PING", timeout=0.5).startswith(b"+PONG"):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"server on :{port} did not come up")


class Server:
    def __init__(self, name, argv, port, cpus):
        self.name, self.port = name, port
        cmd = (["taskset", "-c", cpus] if cpus else []) + argv
        self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_ready(port, self.proc)

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.proc.kill()


def redis_server(port, cpus, aof_dir=None):
    argv = ["redis-server", "--port", str(port), "--save", "", "--protected-mode", "no"]
    if aof_dir:
        argv += ["--appendonly", "yes", "--appendfsync", "always", "--dir", str(aof_dir)]
    else:
        argv += ["--appendonly", "no"]
    return Server("Redis 8 (1 thread)" if not aof_dir else "Redis 8", argv, port, cpus)


def forgekv(port, cpus, shards, aof_dir=None, backend="auto"):
    argv = JAVA + [str(FORGEKV), "--port", str(port), "--shards", str(shards)]
    name = f"ForgeKV {shards} shard" + ("s" if shards > 1 else "")
    if aof_dir:
        argv += ["--appendonly", "--appendfsync", "always", "--aof-backend", backend,
                 "--appendfilename", str(aof_dir / "appendonly.aof")]
        name += f", {backend}"
    srv = Server(name, argv, port, cpus)
    if not aof_dir:
        # Let the JIT compile the hot paths before anything is measured.
        bench(port, None, tests="set,get,incr,lpush,hset", n=300_000, pipeline=16)
    return srv


# --------------------------------------------------------------------------
# redis-benchmark driver
# --------------------------------------------------------------------------

def bench(port, cpus, *, tests=None, command_args=None, n, clients=50, pipeline=1, rand=1_000_000, threads=4):
    argv = (["taskset", "-c", cpus] if cpus else []) + [
        "redis-benchmark", "-p", str(port), "-n", str(n), "-c", str(clients), "-P", str(pipeline),
        "--threads", str(threads), "--csv",
    ]
    if rand:
        argv += ["-r", str(rand)]
    if tests:
        argv += ["-t", tests]
    if command_args:
        argv += command_args
    out = subprocess.run(argv, capture_output=True, text=True, check=True).stdout
    rows = {}
    for row in csv.DictReader(io.StringIO(out)):
        rows[row["test"]] = {
            "rps": float(row["rps"]),
            "p50": float(row["p50_latency_ms"]),
            "p99": float(row["p99_latency_ms"]),
        }
    return rows


def fmt_rps(v):
    return f"{v / 1e6:.2f} M" if v >= 1e6 else f"{v / 1e3:.0f} k"


def table(title, columns, rows, note=None):
    lines = [f"### {title}", ""]
    if note:
        lines += [note, ""]
    lines.append("| | " + " | ".join(columns) + " |")
    lines.append("|---|" + "---:|" * len(columns))
    for label, cells in rows:
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------
# suites
# --------------------------------------------------------------------------

def throughput_suite(a, raw):
    sections = []
    workloads = ["SET", "GET", "INCR", "LPUSH", "HSET", "MSET (10 keys)"]
    for pipeline in (1, 16):
        n = (a.n_p16 if pipeline > 1 else a.n_p1)
        servers = [
            lambda: redis_server(7001, a.server_cpus),
            lambda: forgekv(7002, a.server_cpus, 1),
            lambda: forgekv(7003, a.server_cpus, a.shards),
        ]
        results = []
        for make in servers:
            srv = make()
            try:
                r = bench(srv.port, a.client_cpus, tests="set,get,incr,lpush,hset", n=n, pipeline=pipeline)
                # MSET moves 10 keys per op: fewer ops for a similar run time.
                r.update(bench(srv.port, a.client_cpus, tests="mset", n=n // 5, pipeline=pipeline))
            finally:
                srv.stop()
            results.append((srv.name, r))
            for test, v in r.items():
                raw.append(["throughput", f"P{pipeline}", srv.name, test, v["rps"], v["p50"], v["p99"]])
        base = results[0][1]
        rows = []
        for w in workloads:
            cells = []
            for name, r in results:
                if w not in r:
                    cells.append("–")
                    continue
                v = r[w]
                cell = f"{fmt_rps(v['rps'])} · p99 {v['p99']:.2f} ms"
                if name != results[0][0] and w in base:
                    cell += f" (**{v['rps'] / base[w]['rps']:.2f}×**)"
                cells.append(cell)
            rows.append((w, cells))
        title = "Throughput, no pipelining (`-P 1`)" if pipeline == 1 else f"Throughput, pipelined (`-P {pipeline}`)"
        sections.append(table(title, [name for name, _ in results], rows,
                              note=f"`-c 50 -r 1000000 -n {n:,}` · ops/s · ratio vs Redis"))
    return sections


def flash_sale_suite(a, raw):
    """One hot stock key (worst case for sharding) and 1,000 events."""
    rows = []
    stock = 10**12
    events = [f"stock:{i:012d}" for i in range(1000)]
    cases = [
        ("1 hot event, `-P 1`", 1, "stock:hot", 0),
        ("1 hot event, `-P 16`", 16, "stock:hot", 0),
        ("1,000 events, `-P 16`", 16, "stock:__rand_int__", 1000),
    ]
    servers = [
        ("Redis 8 + Lua", lambda: redis_server(7001, a.server_cpus), True),
        ("ForgeKV 1 shard · DECRIF", lambda: forgekv(7002, a.server_cpus, 1), False),
        (f"ForgeKV {a.shards} shards · DECRIF", lambda: forgekv(7003, a.server_cpus, a.shards), False),
    ]
    results = {}
    for label, make, lua in servers:
        srv = make()
        try:
            command(srv.port, "SET", "stock:hot", stock)
            for i in range(0, len(events), 100):
                parts = ["MSET"]
                for k in events[i:i + 100]:
                    parts += [k, stock]
                command(srv.port, *parts)
            sha = None
            if lua:
                sha = command(srv.port, "SCRIPT", "LOAD", DECRIF_LUA).split(b"\r\n")[1].decode()
            for case, pipeline, key, rand in cases:
                if lua:
                    args = ["EVALSHA", sha, "1", key, "0", "1"]
                else:
                    args = ["DECRIF", key, "0", "1"]
                n = a.n_p16 // 2 if pipeline > 1 else a.n_p1
                r = bench(srv.port, a.client_cpus, command_args=args, n=n, pipeline=pipeline, rand=rand)
                v = next(iter(r.values()))
                results[(label, case)] = v
                raw.append(["flash-sale", case, label, args[0], v["rps"], v["p50"], v["p99"]])
        finally:
            srv.stop()
    base = servers[0][0]
    for case, *_ in cases:
        cells = []
        for label, *_ in servers:
            v = results[(label, case)]
            cell = f"{fmt_rps(v['rps'])} · p99 {v['p99']:.2f} ms"
            if label != base:
                cell += f" (**{v['rps'] / results[(base, case)]['rps']:.2f}×**)"
            cells.append(cell)
        rows.append((case, cells))
    return [table("Flash-sale primitive: decrement-if-in-stock", [s[0] for s in servers], rows,
                  note="Redis has no conditional decrement, so it runs the equivalent Lua script (`EVALSHA`).")]


def durability_suite(a, raw):
    shutil.rmtree(DATA, ignore_errors=True)
    servers = [
        lambda d: redis_server(7001, a.server_cpus, aof_dir=d),
        lambda d: forgekv(7002, a.server_cpus, 1, aof_dir=d, backend="std"),
        lambda d: forgekv(7003, a.server_cpus, 1, aof_dir=d, backend="uring"),
        lambda d: forgekv(7004, a.server_cpus, a.shards, aof_dir=d, backend="uring"),
    ]
    cases = [("SET, `-P 1`", 1, a.n_fsync), ("SET, `-P 16`", 16, a.n_fsync * 8)]
    results = []
    for i, make in enumerate(servers):
        d = DATA / f"s{i}"
        d.mkdir(parents=True)
        srv = make(d)
        try:
            r = {}
            for case, pipeline, n in cases:
                v = bench(srv.port, a.client_cpus, tests="set", n=n, pipeline=pipeline)["SET"]
                r[case] = v
                raw.append(["durability", case, srv.name, "SET", v["rps"], v["p50"], v["p99"]])
        finally:
            srv.stop()
        results.append((srv.name, r))
    shutil.rmtree(DATA, ignore_errors=True)
    rows = []
    for case, *_ in cases:
        cells = []
        for name, r in results:
            v = r[case]
            cell = f"{fmt_rps(v['rps'])} · p99 {v['p99']:.2f} ms"
            if name != results[0][0]:
                cell += f" (**{v['rps'] / results[0][1][case]['rps']:.2f}×**)"
            cells.append(cell)
        rows.append((case, cells))
    return [table("Durable writes: `appendfsync always`", [n for n, _ in results], rows,
                  note="Every acknowledged write is on disk. Group commit decides how many writes share one `fdatasync`.")]


def machine():
    model = cores = ""
    try:
        for line in subprocess.run(["lscpu"], capture_output=True, text=True).stdout.splitlines():
            if line.startswith("Model name:"):
                model = line.split(":", 1)[1].strip()
            if line.startswith("CPU(s):"):
                cores = line.split(":", 1)[1].strip()
    except OSError:
        pass
    redis_v = subprocess.run(["redis-server", "--version"], capture_output=True, text=True).stdout.split()[2]
    return f"{model}, {cores} logical CPUs · Linux {platform.release()} · Redis {redis_v.removeprefix('v=')}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--quick", action="store_true", help="fewer requests per test")
    ap.add_argument("--shards", type=int, default=4, help="shards for the multi-shard ForgeKV runs")
    ap.add_argument("--server-cpus", default="0-3", help="taskset CPU list for servers ('' = no pinning)")
    ap.add_argument("--client-cpus", default="4,5,10,11", help="taskset CPU list for redis-benchmark")
    ap.add_argument("--only", choices=["throughput", "flash", "durability"], action="append")
    ap.add_argument("--no-build", action="store_true")
    ap.add_argument("--out", default=str(ROOT / "bench" / "RESULTS.md"))
    a = ap.parse_args()
    # redis-benchmark --threads measures time in coarse steps, so every test
    # should run for several seconds (~8 s in the full run) to be accurate.
    a.n_p1, a.n_p16, a.n_fsync = (200_000, 2_000_000, 40_000) if a.quick else (1_000_000, 10_000_000, 200_000)

    for tool in ("redis-server", "redis-benchmark", "java", "mvn") + (("taskset",) if a.server_cpus else ()):
        if not shutil.which(tool):
            sys.exit(f"missing required tool: {tool}")
    if not a.no_build:
        subprocess.run(["mvn", "-q", "-DskipTests", "package"], cwd=ROOT, check=True)

    suites = a.only or ["throughput", "flash", "durability"]
    raw, sections = [], []
    started = time.time()
    if "throughput" in suites:
        sections += throughput_suite(a, raw)
    if "flash" in suites:
        sections += flash_sale_suite(a, raw)
    if "durability" in suites:
        sections += durability_suite(a, raw)

    report = [
        "# ForgeKV (Java) vs Redis: benchmark results",
        "",
        f"_Generated by `bench/bench.py{' --quick' if a.quick else ''}` on {time.strftime('%Y-%m-%d')}._",
        "",
        f"**Machine:** {machine()}",
        "",
        f"**Setup:** servers pinned to CPUs `{a.server_cpus or 'any'}`, `redis-benchmark --threads 4` pinned to "
        f"`{a.client_cpus or 'any'}`, 50 connections, random keys over a 1M keyspace. Each cell is ops/s plus p99 "
        f"latency, with the ratio vs Redis in bold. All servers run on one laptop over loopback, so treat the "
        f"numbers as relative, not absolute.",
        "",
    ] + sections
    Path(a.out).write_text("\n".join(report))
    raw_path = Path(a.out).with_suffix(".csv")
    with open(raw_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["suite", "case", "server", "test", "rps", "p50_ms", "p99_ms"])
        w.writerows(raw)
    print(f"wrote {a.out} and {raw_path} in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
