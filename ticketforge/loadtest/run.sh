#!/usr/bin/env bash
# Runs one k6 scenario in Docker, pinned to its own CPUs, while sampling the app and Postgres.
#   ./run.sh <name> <scenario.js> [KEY=VALUE ...]      e.g. ./run.sh s2-atomic-1000 s2-stampede.js STRATEGY=ATOMIC_CONDITIONAL VUS=1000
# Results: results/<name>.json (k6 summary), results/<name>.samples.jsonl, results/<name>.invariants.json
set -euo pipefail
cd "$(dirname "$0")"
name=$1; script=$2; shift 2
K6_CPUS=${K6_CPUS:-4,5,10,11}
PG_CONTAINER=${PG_CONTAINER:-tf-pg}
envs=(); for kv in "$@"; do envs+=(-e "$kv"); done

python3 -I sampler.py "results/$name.samples.jsonl" "$PG_CONTAINER" & sampler=$!
trap 'kill $sampler 2>/dev/null || true' EXIT

docker run --rm --network host --cpuset-cpus "$K6_CPUS" -u "$(id -u):$(id -g)" \
  -v "$PWD":/work -w /work "${envs[@]}" grafana/k6:1.8.2 run --quiet \
  --summary-trend-stats "avg,min,med,max,p(90),p(95),p(99)" \
  --summary-export "results/$name.json" "scenarios/$script" 2>&1 | grep -vE '^\s*$' | tail -60 || true

kill $sampler 2>/dev/null || true
admin=$(curl -s -XPOST localhost:8080/auth/login -H 'Content-Type: application/json' \
  -d '{"email":"admin@ticketforge.local","password":"admin12345"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["accessToken"])')
curl -s localhost:8080/admin/invariants -H "Authorization: Bearer $admin" > "results/$name.invariants.json"
echo "invariants: $(cat "results/$name.invariants.json")"
