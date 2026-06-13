#!/usr/bin/env bash
set -euo pipefail

PROFILE="${PROFILE:-akka-7253}"
NAMESPACE="${NAMESPACE:-akka-7253}"
CPUS="${MINIKUBE_CPUS:-8}"
MEMORY="${MINIKUBE_MEMORY:-24576}"
DISK="${MINIKUBE_DISK:-60g}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need docker
need kubectl
need minikube
need dotnet
need curl
need python3

if ! docker info >/dev/null 2>&1; then
  echo "docker is not reachable; fix Docker permissions/context before running the harness" >&2
  exit 1
fi

if ! minikube -p "$PROFILE" status >/dev/null 2>&1; then
  minikube start -p "$PROFILE" --driver=docker --cpus="$CPUS" --memory="$MEMORY" --disk-size="$DISK"
fi

minikube -p "$PROFILE" update-context >/dev/null
kubectl config use-context "$PROFILE" >/dev/null

current_context="$(kubectl config current-context)"
if [[ "$current_context" != "$PROFILE" ]]; then
  echo "refusing to run against kubectl context '$current_context'; expected '$PROFILE'" >&2
  exit 1
fi

kubectl apply -f "$(dirname "$0")/../k8s/00-namespace.yaml" >/dev/null
kubectl get namespace "$NAMESPACE" >/dev/null

host_nofile="$(ulimit -n)"
if [[ "$host_nofile" -lt 65536 ]]; then
  echo "warning: host ulimit -n is $host_nofile; 20k TCP connections may require >= 65536" >&2
fi

echo "preflight ok: profile=$PROFILE namespace=$NAMESPACE dotnet=$(dotnet --version)"
