#!/usr/bin/env bash
# Starts the four services on the host, pinned to the application cores, with the heap split so the
# total matches the monolith's 1.5 GB. Infrastructure must already be up (docker compose, see deploy/).
#   scripts/run-stack.sh          start      scripts/stop-stack.sh   stop
set -euo pipefail
cd "$(dirname "$0")/.."
CPUS=${APP_CPUS:-0,1,6,7}
LOGS=${LOG_DIR:-target/stack-logs}; mkdir -p "$LOGS" target
export LOG_FORMAT=${LOG_FORMAT:-ecs}
export REDIS_PORT=${REDIS_PORT:-6380}   # see deploy/docker-compose.yml

start() {  # name port heap
  local name=$1 port=$2 heap=$3 jar
  jar=$(ls "$name"/target/"$name"-*-SNAPSHOT.jar | head -1)
  PORT=$port nohup taskset -c "$CPUS" java -Xmx"$heap" -jar "$jar" > "$LOGS/$name.log" 2>&1 &
  echo $! > "target/$name.pid"
  echo "started $name on :$port (heap $heap, pid $(cat target/$name.pid))"
}

start auth-service        8081 128m
start catalog-service     8082 384m
start reservation-service 8083 768m
start booking-service     8084 256m

for p in 8081 8082 8083 8084; do
  for i in $(seq 1 60); do
    curl -fsS "localhost:$p/actuator/health" >/dev/null 2>&1 && { echo ":$p healthy"; break; }
    sleep 2; [ "$i" = 60 ] && { echo ":$p did not become healthy"; exit 1; }
  done
done
