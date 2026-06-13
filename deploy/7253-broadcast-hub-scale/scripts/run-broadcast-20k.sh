#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
LAB_ROOT="$(cd "$REPO_ROOT/../../.." && pwd)"
PROFILE="${PROFILE:-akka-7253}"
NAMESPACE="${NAMESPACE:-akka-7253}"
TARGET_CONSUMERS="${TARGET_CONSUMERS:-20000}"
CONNECTIONS_PER_POD="${CONNECTIONS_PER_POD:-100}"
CONSUMER_REPLICAS="${CONSUMER_REPLICAS:-200}"
MODE="${1:-filter}"
RUN_ROOT="${RUN_ROOT:-$LAB_ROOT/runs/akka-7253-broadcast-hub-k8s}"

run_one() {
  local mode="$1"
  local message_count expected_messages expected_deliveries run_dir port_forward_pid

  if [[ "$mode" == "lockstep" ]]; then
    message_count="${LOCKSTEP_MESSAGE_COUNT:-16}"
    expected_messages="$message_count"
    expected_deliveries=$((TARGET_CONSUMERS * message_count))
  elif [[ "$mode" == "filter" ]]; then
    message_count="${FILTER_MESSAGE_COUNT:-$TARGET_CONSUMERS}"
    expected_messages=1
    expected_deliveries="$TARGET_CONSUMERS"
  else
    echo "mode must be 'filter', 'lockstep', or 'all'" >&2
    exit 1
  fi

  run_dir="$RUN_ROOT/$(date -u +%Y%m%dT%H%M%SZ)-$mode"
  mkdir -p "$run_dir"

  "$SCRIPT_DIR/build-images.sh"

  kubectl apply -f "$REPO_ROOT/deploy/7253-broadcast-hub-scale/k8s/00-namespace.yaml" >/dev/null
  kubectl -n "$NAMESPACE" delete statefulset cluster-sanity --ignore-not-found=true >/dev/null
  kubectl -n "$NAMESPACE" delete service cluster-sanity --ignore-not-found=true >/dev/null
  kubectl -n "$NAMESPACE" delete statefulset broadcast-hub-consumer --ignore-not-found=true >/dev/null
  kubectl -n "$NAMESPACE" delete deployment broadcast-hub-broadcaster --ignore-not-found=true >/dev/null
  kubectl -n "$NAMESPACE" delete service broadcast-hub-broadcaster broadcast-hub-consumer --ignore-not-found=true >/dev/null
  kubectl apply -f "$REPO_ROOT/deploy/7253-broadcast-hub-scale/k8s/10-broadcast-hub.yaml" >/dev/null

  kubectl -n "$NAMESPACE" set env deployment/broadcast-hub-broadcaster \
    TARGET_CONSUMERS="$TARGET_CONSUMERS" \
    LOCKSTEP_MESSAGE_COUNT="${LOCKSTEP_MESSAGE_COUNT:-16}" \
    FILTER_MESSAGE_COUNT="${FILTER_MESSAGE_COUNT:-$TARGET_CONSUMERS}" >/dev/null

  kubectl -n "$NAMESPACE" set env statefulset/broadcast-hub-consumer \
    CONNECTIONS_PER_POD="$CONNECTIONS_PER_POD" \
    MODE="$mode" \
    EXPECTED_MESSAGES="$expected_messages" >/dev/null

  kubectl -n "$NAMESPACE" rollout status deployment/broadcast-hub-broadcaster --timeout=300s
  kubectl -n "$NAMESPACE" scale statefulset/broadcast-hub-consumer --replicas="$CONSUMER_REPLICAS" >/dev/null
  kubectl -n "$NAMESPACE" rollout status statefulset/broadcast-hub-consumer --timeout="${CONSUMER_ROLLOUT_TIMEOUT:-1200s}"

  kubectl -n "$NAMESPACE" port-forward service/broadcast-hub-broadcaster 18080:8080 > "$run_dir/port-forward.log" 2>&1 &
  port_forward_pid="$!"
  trap 'kill "$port_forward_pid" >/dev/null 2>&1 || true' RETURN
  sleep 2

  wait_metric "materializedConsumers" "$TARGET_CONSUMERS" "$run_dir/metrics-before.json" "${ATTACH_TIMEOUT_SECONDS:-1200}"

  curl -fsS "http://127.0.0.1:18080/metrics" > "$run_dir/metrics-before.json"
  curl -fsS -X POST "http://127.0.0.1:18080/run?mode=$mode&messageCount=$message_count" > "$run_dir/run-result.json"
  wait_metric "deliveredWrites" "$expected_deliveries" "$run_dir/metrics-after.json" "${RUN_TIMEOUT_SECONDS:-1200}"
  curl -fsS "http://127.0.0.1:18080/metrics" > "$run_dir/metrics-after.json"

  RUN_DIR="$run_dir" MODE="$mode" SCENARIO="broadcast" "$SCRIPT_DIR/collect-report.sh" >/dev/null
  echo "broadcast $mode report: $run_dir/report.md"
}

wait_metric() {
  local field="$1"
  local expected="$2"
  local output="$3"
  local timeout_seconds="$4"
  local start now value

  start="$(date +%s)"
  while true; do
    curl -fsS "http://127.0.0.1:18080/metrics" > "$output"
    value="$(python3 - "$field" "$output" <<'PY'
import json
import sys
field = sys.argv[1]
path = sys.argv[2]
with open(path, "r", encoding="utf-8") as handle:
    data = json.load(handle)
print(int(data.get(field, 0)))
PY
)"
    if [[ "$value" -ge "$expected" ]]; then
      return 0
    fi

    now="$(date +%s)"
    if (( now - start > timeout_seconds )); then
      echo "timed out waiting for $field >= $expected; last value=$value" >&2
      return 1
    fi

    sleep 5
  done
}

if [[ "$MODE" == "all" ]]; then
  run_one lockstep
  run_one filter
else
  run_one "$MODE"
fi
