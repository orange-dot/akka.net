#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
LAB_ROOT="$(cd "$REPO_ROOT/../../.." && pwd)"
NAMESPACE="${NAMESPACE:-akka-7253}"
RUN_DIR="${RUN_DIR:-$LAB_ROOT/runs/akka-7253-broadcast-hub-k8s/$(date -u +%Y%m%dT%H%M%SZ)}"
MODE="${MODE:-unknown}"
SCENARIO="${SCENARIO:-broadcast}"

mkdir -p "$RUN_DIR"

kubectl -n "$NAMESPACE" get pods -o wide > "$RUN_DIR/pods.txt" || true
kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp > "$RUN_DIR/events.txt" || true

if kubectl -n "$NAMESPACE" get deployment broadcast-hub-broadcaster >/dev/null 2>&1; then
  kubectl -n "$NAMESPACE" logs deployment/broadcast-hub-broadcaster --tail=-1 > "$RUN_DIR/broadcaster.log" || true
fi

if kubectl -n "$NAMESPACE" get statefulset broadcast-hub-consumer >/dev/null 2>&1; then
  kubectl -n "$NAMESPACE" logs -l app=broadcast-hub-consumer --tail=-1 --all-containers=true --prefix=true --max-log-requests=250 > "$RUN_DIR/consumers.log" || true
fi

if kubectl -n "$NAMESPACE" get statefulset cluster-sanity >/dev/null 2>&1; then
  kubectl -n "$NAMESPACE" logs -l app=cluster-sanity --tail=-1 --all-containers=true --prefix=true > "$RUN_DIR/cluster-sanity.log" || true
fi

cat > "$RUN_DIR/report.md" <<REPORT
# Akka.NET #7253 Local Kubernetes Report

- Scenario: \`$SCENARIO\`
- Mode: \`$MODE\`
- Timestamp UTC: \`$(date -u +%Y-%m-%dT%H:%M:%SZ)\`
- Repository: \`$REPO_ROOT\`
- Branch: \`$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)\`
- Git SHA: \`$(git -C "$REPO_ROOT" rev-parse HEAD)\`
- Kubernetes context: \`$(kubectl config current-context)\`
- Namespace: \`$NAMESPACE\`
- .NET SDK: \`$(dotnet --version)\`

## Evidence Boundary

The BroadcastHub lane is the primary issue #7253 evidence: Kubernetes clients create TCP connections, and the broadcaster process creates the real BroadcastHub downstream materializations.

The cluster sanity lane validates Akka.Cluster DistributedPubSub fanout under local Kubernetes only. It is not evidence for the BroadcastHub wheel-bucket fix.

## Raw Artifacts

- \`pods.txt\`
- \`events.txt\`
- \`broadcaster.log\` when present
- \`consumers.log\` when present
- \`cluster-sanity.log\` when present
- \`metrics-before.json\`, \`run-result.json\`, and \`metrics-after.json\` when present
REPORT

echo "$RUN_DIR/report.md"
