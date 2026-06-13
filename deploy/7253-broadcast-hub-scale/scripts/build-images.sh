#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PROFILE="${PROFILE:-akka-7253}"

"$SCRIPT_DIR/preflight.sh"

eval "$(minikube -p "$PROFILE" docker-env)"

docker build -t akka-7253/broadcast-hub-broadcaster:local -f "$REPO_ROOT/deploy/7253-broadcast-hub-scale/docker/Dockerfile.broadcaster" "$REPO_ROOT"
docker build -t akka-7253/broadcast-hub-consumer:local -f "$REPO_ROOT/deploy/7253-broadcast-hub-scale/docker/Dockerfile.consumer" "$REPO_ROOT"
docker build -t akka-7253/cluster-sanity:local -f "$REPO_ROOT/deploy/7253-broadcast-hub-scale/docker/Dockerfile.cluster-sanity" "$REPO_ROOT"

echo "images built in minikube docker daemon for profile=$PROFILE"
