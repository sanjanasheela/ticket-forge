#!/usr/bin/env bash
# Chaos actions for Mission Control. Usage: mc-chaos.sh kill-cache|start-cache|kill-<svc>|start-<svc>
cd "$(dirname "$0")/.."
act=$1; svc=${act#*-}
case "$act" in
  kill-cache)  fuser -k "${REDIS_PORT:-6380}/tcp" >/dev/null 2>&1 && echo "killed cache on ${REDIS_PORT:-6380}" || docker stop tf-redis >/dev/null 2>&1 && echo "stopped tf-redis" ;;
  start-cache) docker start tf-redis >/dev/null 2>&1 && echo "started tf-redis" ;;
  kill-auth|kill-catalog|kill-reservation|kill-booking)
      [ -f "target/$svc-service.pid" ] && kill -9 "$(cat target/$svc-service.pid)" && echo "killed $svc-service" || echo "no pid file" ;;
  start-auth|start-catalog|start-reservation|start-booking)
      declare -A P=([auth]=8081 [catalog]=8082 [reservation]=8083 [booking]=8084)
      jar=$(ls $svc-service/target/$svc-service-*-SNAPSHOT.jar | head -1)
      PORT=${P[$svc]} REDIS_PORT=${REDIS_PORT:-6380} nohup java -jar "$jar" > "target/stack-logs/$svc-service.log" 2>&1 &
      echo $! > "target/$svc-service.pid"; echo "started $svc-service" ;;
  *) echo "unknown"; exit 1 ;;
esac
