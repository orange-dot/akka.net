# Akka.NET v1.6 Breaking Changes

This document tracks **breaking changes** introduced into the `dev` branch during the
Akka.NET **v1.6** development cycle, ahead of a stable `v1.6.0` release.

A change is **breaking** — and therefore **must** be recorded here — if it alters any of:

- **Behavior** — observable runtime semantics change (e.g. a message that used to do `X`
  now does `Y`, or is now ignored).
- **Wire** — serialization or network-protocol compatibility with prior `1.x` releases
  changes.
- **API** — a public type or member is removed, renamed, or has its signature/contract
  changed (anything `Akka.API.Tests` would flag).

## Scope and lifecycle

- This ledger is maintained **only until a stable `v1.6.0` ships**. At that point its
  contents are folded into the official release notes / upgrade guide and this file is
  retired.
- Record the entry in the **same PR** that introduces the breaking change, so reviewers can
  weigh the break alongside the code.
- A change still in flight may be listed with status `Planned` and its branch name; update
  it to `Merged` with the PR link once it lands on `dev`.
- Bug fixes that merely restore documented/intended behavior are not "breaking" for this
  list — but note them anyway if users may have come to depend on the old (incorrect)
  behavior.

## Entry format

Newest first. Keep `Change` to a line or two; link the PR for detail. `Type` is one or more
of `Behavior`, `Wire`, `API` (combine with `+`).

| Status | PR / Branch | Component | Type | Change | Migration |
|--------|-------------|-----------|------|--------|-----------|
| Planned / Merged | link or branch | `Akka.Xyz` | Behavior | one-line summary | what users must do |

---

## Changes

| Status | PR / Branch | Component | Type | Change | Migration |
|--------|-------------|-----------|------|--------|-----------|
| Planned | `claude-5381-subflow-typed-subtypes` | `Akka.Streams` | API | `Source`/`Flow` `GroupBy`/`SplitWhen`/`SplitAfter` now return the new `SourceSubFlow<TOut,TMat>` / `FlowSubFlow<TIn,TOut,TMat>` subtypes of `SubFlow`, and `MergeSubstreams`/`MergeSubstreamsWithParallelism`/`ConcatSubstream` on those subtypes return the concrete `Source`/`Flow` instead of `IFlow` (issue #5381). Source-compatible (the subtypes are assignable to `SubFlow`); binary-breaking on the changed return types. | Recompile. Casts previously required after `MergeSubstreams` (e.g. `((Source<...>)source)`) can be removed. Materialized-value operators (`AlsoToMaterialized`, `DivertToMaterialized`, `WatchTermination`, `InterleaveMaterialized`, `MergeMaterialized`) are not mirrored on the typed subtypes and resolve to the base `SubFlowOperations` as before. |
| Planned | `feature/deprecate-actorpublisher` | `Akka.Streams` | Behavior + API | `Source.ActorRef<T>` is being re-implemented as a stream-native `GraphStage` (off the legacy `ActorPublisher`). The materialized `IActorRef` no longer treats `PoisonPill` / `Kill` as stream completion — those messages are now ignored, consistent with stage-actor semantics. | Complete the stream explicitly: send `new Status.Success(...)` to drain buffered elements and complete, or `new Status.Failure(ex)` to fail. Do not rely on `PoisonPill` / `Kill`. |
