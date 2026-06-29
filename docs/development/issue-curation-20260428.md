# Akka.NET Issue Curation

Date: 2026-04-28

Repository:

- Local clone: `/home/dev/work-base-20260421/workspace/platform/akka.net`
- Fork remote: `origin https://github.com/orange-dot/akka.net.git`
- Upstream remote: `upstream https://github.com/akkadotnet/akka.net.git`
- Base branch: `dev`

Status: initial read-only curation. No branch, commit, push, issue comment, or upstream PR yet.

## Selection Frame

Prefer issues that are:

- current enough that maintainers may still care,
- small enough for a first Akka.NET PR,
- testable locally,
- aligned with actor/streams/runtime quality,
- not dependent on a large maintainer design decision.

Avoid, for now:

- broad architecture epics,
- large performance rewrites,
- cluster/protocol correctness issues without a reproducer,
- changes that would require guessing public API policy before discussion.

## Strong Candidates

### #8081: Source.Queue `OfferAsync` needs `CancellationToken` / timeout support

URL: https://github.com/akkadotnet/akka.net/issues/8081

Labels observed: `up for grabs`, `akka-streams`, `api-change`, `good for first-time contributors`

Why it is attractive:

- Recent issue.
- Maintainer labels signal it is acceptable for contributors.
- Clear user pain: indefinite await under backpressure can wedge async actor handlers.
- Good technical fit with our Temporal/Akka interest in cancellation, backpressure, and durable runtime boundaries.

Risk:

- Public API extension, so API approval tests are required.
- Need to understand `ISourceQueueWithComplete<T>` and stream queue internals before choosing exact overload shape.

Local finding:

- API entrypoint: `src/core/Akka.Streams/Queue.cs`
- Implementation: `src/core/Akka.Streams/Implementation/Sources.cs`, nested `QueueSource<TOut>.Materialized.OfferAsync`.
- Tests: `src/core/Akka.Streams.Tests/Dsl/QueueSourceSpec.cs`
- API approval snapshots already expose `ISourceQueue.OfferAsync(T element)`, so any overload requires stream API approval updates.

First local triage:

- Find `ISourceQueueWithComplete<T>`, `OfferAsync`, and queue materialization code.
- Check whether `Source.Channel` already solves this and can guide semantics.
- Identify tests around backpressure queue offer behavior.

### #5202: Port cause parameter for `OnDownstreamFinished`

URL: https://github.com/akkadotnet/akka.net/issues/5202

Labels observed: `up for grabs`, `enhancement`, `akka-streams`

Why it is attractive:

- Explicit port-from-JVM issue.
- Likely bounded to stream stage lifecycle APIs and tests.
- Up-for-grabs label lowers social risk.

Local finding:

- `rg` shows `OnDownstreamFinish(Exception cause)` already exists broadly under `src/core/Akka.Streams`.
- This issue may be stale, already implemented, or only partially applicable to a narrower legacy API surface.

Risk:

- Public API / protected API changes may ripple through many stages.
- Needs comparison with current Akka JVM semantics before implementation.

First local triage:

- Locate `GraphStageLogic.OnDownstreamFinish` / `OnDownstreamFinished`.
- Count override sites.
- Decide whether compatibility overloads can be added without breaking existing overrides.

Current disposition: de-rank until we confirm whether anything remains to do.

### #5113: Akka.Streams docs for Rx interop

URL: https://github.com/akkadotnet/akka.net/issues/5113

Labels observed: `up for grabs`, `docs`, `akka-streams`

Why it is attractive:

- Low-risk docs contribution.
- Good warmup for learning docs build and stream docs layout.
- Can become a small first PR if code issues need more investigation.

Local finding:

- `docs/articles/streams/integration.md` already contains `Integrating with Observables` and `Integrating with Reactive Streams`.
- This issue may be stale, or it may specifically want a richer Rx.NET guide beyond the current Observable examples.

Risk:

- Old issue; need verify current docs do not already cover this.
- Need examples that compile or match existing docs style.

First local triage:

- Search docs for Rx / Reactive Extensions / interop.
- Find any sample projects used by docs.
- Decide whether a docs-only PR is still valuable.

Current disposition: fallback only, not main target.

## Fresh Actionable Candidates

### #8191: `Akka.TestKit.Xunit` v3 `DisposeAsync` trap leaks ActorSystems

URL: https://github.com/akkadotnet/akka.net/issues/8191

Why it is attractive:

- Very recent and detailed.
- Clear minimal repro and proposed fix options.
- TestKit cleanup leak is a concrete quality issue.

Risk:

- Issue author explicitly calls out source-breaking behavior for some consumers.
- Needs maintainer preference before choosing full fix vs minimal fix.

Likely first move:

- Local reproduction and minimal patch exploration only.
- Do not upstream PR until we can articulate compatibility tradeoff.

### #8187: `Akka.Persistence.TCK.Xunit2` assembly name mismatch

URL: https://github.com/akkadotnet/akka.net/issues/8187

Why it is attractive:

- Recent, concrete packaging/runtime problem.
- Repro shape is understandable: assembly-qualified names break when compat package assembly name changes.

Risk:

- Packaging / assembly identity changes can have wide effects.
- Need inspect project files and release packaging conventions before touching.

Likely first move:

- Local code archaeology: compare `Akka.Persistence.TCK` and `.Xunit2` csproj outputs.
- Determine whether assembly-name change is feasible without breaking package layout.

### #7931: Tutorial code does not compile

URL: https://github.com/akkadotnet/akka.net/issues/7931

Why it is attractive:

- Recent docs bug with maintainer comment pointing toward deleting stale code and sending users to Learn Akka.NET.
- Could be a small cleanup PR.

Risk:

- Need decide whether to delete, replace, or redirect docs.
- Docs direction may be maintainer taste-heavy.

Likely first move:

- Locate tutorial section.
- Identify exact broken snippet.
- Propose minimal docs update before coding.

## Hold / Later

### #7901 / #7902: AOT compatibility work

High-value, modern .NET topic, but too broad for first pass. It likely needs design, source generation / setup API decisions, and many API approvals.

### #8015: Gossip corrupted Seen state / tombstones

Important and labelled confirmed bug, but cluster correctness is high blast radius. Better after we know the cluster internals.

### #6947 / #7611: Akka.Streams memory leaks

Interesting, but memory-leak proof work needs profiling artifacts and careful object graph analysis. Good later if we want an evidence-heavy investigation.

### #7253: BroadcastHub performance with thousands of consumers

Probably benchmark-heavy and design-sensitive. Hold until after a smaller Streams contribution.

### #5103 / #7250: DistributedData durable store / ORSet size behavior

Real distributed systems concerns, but likely require design discussion and deeper DData knowledge.

## Current Shortlist

1. `#8081` as the main implementation candidate.
2. `#8191` as a fresh investigation candidate, but not a blind PR due compatibility risk.
3. `#8187` as a packaging investigation candidate.
4. `#7931` as docs warmup / fallback.
5. `#5202` and `#5113` only after confirming they are not stale.

## Resume Commands

```bash
cd /home/dev/work-base-20260421/workspace/platform/akka.net
git status --short --branch
git remote -v
rg -n "ISourceQueueWithComplete|OfferAsync|SourceQueue" src
rg -n "OnDownstreamFinish|OnDownstreamFinished" src
```

Do not push, comment, or open an upstream PR until the target issue is selected and the operator confirms the path.
