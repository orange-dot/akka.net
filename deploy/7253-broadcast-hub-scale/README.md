# BroadcastHub 20k Kubernetes Harness

This harness is local evidence tooling for issue #7253. It is intentionally kept outside
the upstream PR stack and does not change Akka.NET public APIs.

## BroadcastHub Lane

The primary scenario creates 20,000 real `BroadcastHub` downstream materializations inside
the broadcaster process. Kubernetes consumer pods are thin C# TCP clients:

- 1 broadcaster pod runs Akka.Streams and materializes `Source.Queue<T>` into
  `BroadcastHub.Sink<T>(startAfterNrOfConsumers: 20000, bufferSize: 1024)`.
- 200 consumer pods open 100 TCP connections each.
- Each accepted TCP connection materializes one `hubSource` consumer.
- `lockstep` mode broadcasts a small finite sequence to every connection.
- `filter` mode gives each connection a unique watched value and applies
  `hubSource.Where(x => x.Sequence == watchedValue)`.

Run both broadcast scenarios:

```bash
deploy/7253-broadcast-hub-scale/scripts/run-broadcast-20k.sh all
```

Run one mode:

```bash
deploy/7253-broadcast-hub-scale/scripts/run-broadcast-20k.sh filter
deploy/7253-broadcast-hub-scale/scripts/run-broadcast-20k.sh lockstep
```

Reports are written under `runs/akka-7253-broadcast-hub-k8s/<timestamp>/`.

## Cluster Sanity Lane

The cluster sanity scenario validates Akka.Cluster + DistributedPubSub fanout in the same
local Kubernetes profile. It is not evidence for the BroadcastHub wheel-bucket behavior.

```bash
deploy/7253-broadcast-hub-scale/scripts/run-cluster-sanity.sh
```

## Local Kubernetes

Scripts use an isolated Minikube profile and namespace:

- profile: `akka-7253`
- namespace: `akka-7253`

They fail fast if the current `kubectl` context is not the dedicated profile.
