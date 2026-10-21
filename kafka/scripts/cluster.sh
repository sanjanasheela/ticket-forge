#!/usr/bin/env bash
# Start a 3-broker cluster as three separate processes (so a broker can be
# kill -9'd). Logs and data go to ./run; stop with: scripts/cluster.sh stop
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER="1=127.0.0.1:9091,2=127.0.0.1:9092,3=127.0.0.1:9093"
mkdir -p run
case "${1:-start}" in
  start)
    for id in 1 2 3; do
      ./forgelog broker --id "$id" --cluster "$CLUSTER" --data-dir "run/data-$id" > "run/broker-$id.log" 2>&1 &
      echo $! > "run/broker-$id.pid"
      echo "broker $id: pid $(cat run/broker-$id.pid), log run/broker-$id.log"
    done ;;
  stop)
    for id in 1 2 3; do
      [[ -f "run/broker-$id.pid" ]] && kill "$(cat run/broker-$id.pid)" 2>/dev/null || true
      rm -f "run/broker-$id.pid"
    done ;;
  kill)   # kill -9 one broker: scripts/cluster.sh kill 2
    kill -9 "$(cat "run/broker-$2.pid")" && rm -f "run/broker-$2.pid" && echo "broker $2 killed" ;;
  restart) # scripts/cluster.sh restart 2
    ./forgelog broker --id "$2" --cluster "$CLUSTER" --data-dir "run/data-$2" >> "run/broker-$2.log" 2>&1 &
    echo $! > "run/broker-$2.pid"; echo "broker $2 restarted (pid $!)" ;;
  *) echo "usage: $0 start|stop|kill ID|restart ID" >&2; exit 2 ;;
esac
