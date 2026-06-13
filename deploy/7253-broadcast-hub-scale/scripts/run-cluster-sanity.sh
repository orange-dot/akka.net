#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
LAB_ROOT="$(cd "$REPO_ROOT/../../.." && pwd)"
NAMESPACE="${NAMESPACE:-akka-7253}"
RUN_ROOT="${RUN_ROOT:-$LAB_ROOT/runs/akka-7253-broadcast-hub-k8s}"
RUN_DIR="$RUN_ROOT/$(date -u +%Y%m%dT%H%M%SZ)-cluster-sanity"

mkdir -p "$RUN_DIR"

"$SCRIPT_DIR/build-images.sh"

kubectl apply -f "$REPO_ROOT/deploy/7253-broadcast-hub-scale/k8s/00-namespace.yaml" >/dev/null
kubectl -n "$NAMESPACE" delete statefulset broadcast-hub-consumer --ignore-not-found=true >/dev/null
kubectl -n "$NAMESPACE" delete deployment broadcast-hub-broadcaster --ignore-not-found=true >/dev/null
kubectl -n "$NAMESPACE" delete service broadcast-hub-broadcaster broadcast-hub-consumer --ignore-not-found=true >/dev/null
kubectl -n "$NAMESPACE" delete statefulset cluster-sanity --ignore-not-found=true >/dev/null
kubectl -n "$NAMESPACE" delete service cluster-sanity --ignore-not-found=true >/dev/null
kubectl apply -f "$REPO_ROOT/deploy/7253-broadcast-hub-scale/k8s/20-cluster-sanity.yaml" >/dev/null
kubectl -n "$NAMESPACE" rollout status statefulset/cluster-sanity --timeout="${CLUSTER_ROLLOUT_TIMEOUT:-600s}"

sleep "${CLUSTER_CAPTURE_SECONDS:-120}"

RUN_DIR="$RUN_DIR" MODE="distributed-pubsub" SCENARIO="cluster-sanity" "$SCRIPT_DIR/collect-report.sh" >/dev/null
echo "cluster sanity report: $RUN_DIR/report.md"
