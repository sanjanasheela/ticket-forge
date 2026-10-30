#!/usr/bin/env bash
# Stops the services started by run-stack.sh (by pid file, so nothing else is touched).
cd "$(dirname "$0")/.."
for f in target/*-service.pid; do
  [ -e "$f" ] || continue
  pid=$(cat "$f"); kill "$pid" 2>/dev/null && echo "stopped $(basename "$f" .pid) ($pid)"; : > "$f"
done
