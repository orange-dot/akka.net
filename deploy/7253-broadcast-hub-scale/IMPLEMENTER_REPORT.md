# Implementer Report: BroadcastHub 20k Consumer Scenario

## Purpose

This harness exists to collect local Kubernetes evidence for Akka.NET issue #7253
without changing the upstream BroadcastHub PR stack.

The primary lane measures `BroadcastHub` behavior. The secondary lane checks
Akka.Cluster + DistributedPubSub fanout under the same local Kubernetes profile, but
that secondary result must not be used as evidence for the BroadcastHub wheel-bucket
implementation.

## 20k Consumer Shape

- Kubernetes profile: `akka-7253`
- Namespace: `akka-7253`
- Broadcaster: 1 pod
- Consumers: 200 StatefulSet pods
- Connections per consumer pod: 100
- Total logical stream consumers: 20,000

Each consumer pod opens 100 TCP connections to `broadcast-hub-broadcaster:7000`.
For each accepted TCP connection, the broadcaster materializes one downstream
`hubSource` from:

```csharp
Source.Queue<HubEvent>(queueBufferSize, OverflowStrategy.Backpressure)
    .ToMaterialized(
        BroadcastHub.Sink<HubEvent>(
            startAfterNrOfConsumers: 20000,
            bufferSize: 1024),
        Keep.Both)
```

This means the 20k cardinality is counted inside the Akka.Streams `BroadcastHub`
process, not as 20k Kubernetes pods.

## Broadcast Modes

`filter` is the issue-shaped default:

- Pod ordinal comes from StatefulSet names such as `broadcast-hub-consumer-42`.
- Watched value base is `podOrdinal * CONNECTIONS_PER_POD`.
- Connection `i` watches `base + i`.
- The broadcaster applies `hubSource.Where(evt => evt.Sequence == watchedValue)`.
- The producer sends `0..19999`; each connection should receive one matching line.

`lockstep` is the pure broadcast sanity mode:

- All consumers attach before the producer starts.
- The producer sends a small finite count, default `16`.
- Every connection should receive all lockstep messages.

## Commands

Run both BroadcastHub modes:

```bash
deploy/7253-broadcast-hub-scale/scripts/run-broadcast-20k.sh all
```

Run the primary issue-shaped mode only:

```bash
deploy/7253-broadcast-hub-scale/scripts/run-broadcast-20k.sh filter
```

Run the secondary cluster sanity lane:

```bash
deploy/7253-broadcast-hub-scale/scripts/run-cluster-sanity.sh
```

## Report Output

Scripts write reports under:

```text
runs/akka-7253-broadcast-hub-k8s/<timestamp>/report.md
```

Each run directory also captures pod status, Kubernetes events, broadcaster logs,
consumer logs, and BroadcastHub metrics snapshots when available.

For #7253 PR notes, cite the BroadcastHub lane only. Keep any cluster sanity numbers in
a separate section labelled as operational Kubernetes sanity.
