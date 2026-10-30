#!/usr/bin/env bash
# Runs one k6 scenario against Kong in Docker, pinned to its own CPUs, while sampling the whole stack.
#   loadtest/run.sh <name> <scenario.js> [KEY=VALUE ...]     e.g. loadtest/run.sh s2-atomic-1000 s2-stampede.js STRATEGY=ATOMIC_CONDITIONAL VUS=1000
# Same k6 scripts as Phase 1 (copied unchanged). Results: results/<name>.json, .samples.jsonl, .invariants.json
set -euo pipefail
cd "$(dirname "$0")"
name=$1; script=$2; shift 2
K6_CPUS=${K6_CPUS:-4,5,10,11}
envs=(); for kv in "$@"; do envs+=(-e "$kv"); done
root=$(cd .. && pwd)

python3 -I sampler.py "results/$name.samples.jsonl" "$root" & sampler=$!
trap 'kill $sampler 2>/dev/null || true' EXIT

docker run --rm --network host --cpuset-cpus "$K6_CPUS" -u "$(id -u):$(id -g)" \
  -v "$PWD":/work -w /work "${envs[@]}" grafana/k6:1.8.2 run --quiet \
  --summary-trend-stats "avg,min,med,max,p(90),p(95),p(99)" \
  --summary-export "results/$name.json" "scenarios/$script" 2>&1 | grep -vE '^\s*$' | tail -60 || true

kill $sampler 2>/dev/null || true
# distributed invariants settle once in-flight sagas finish; give them a moment, then check all nine
sleep "${SETTLE_WAIT:-8}"
python3 -I ../scripts/verify-invariants.py "${SETTLE:-5}" > "results/$name.invariants.json" || true
echo "invariants: $(cat "results/$name.invariants.json")"
