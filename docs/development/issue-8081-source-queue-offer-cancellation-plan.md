# Issue 8081: Source.Queue OfferAsync Cancellation Plan

Date: 2026-04-28

Branch: `fix/8081-source-queue-offer-cancellation`

Issue: https://github.com/akkadotnet/akka.net/issues/8081

Status: local planning / code reading. No implementation yet. No push.

Review adjustment: do not add a required member to the existing
`ISourceQueueWithComplete<T>` interface. The implementation should expose
the token-aware offer through a new opt-in derived interface,
`ICancellableSourceQueueWithComplete<T>`, plus an extension method on
`ISourceQueueWithComplete<T>` that delegates only when the materialized
queue supports the capability.

## Problem Summary

`Source.Queue` materializes as `ISourceQueueWithComplete<T>`. Its inherited `OfferAsync(T element)` can remain incomplete indefinitely when `Source.Queue` uses `OverflowStrategy.Backpressure` and the downstream side stops demanding elements.

This is especially risky when callers await `OfferAsync` inside actor handlers such as `ReceiveAsync`: the actor can stop making progress while it waits for demand that might never arrive.

## Local Code Map

Public API:

- `src/core/Akka.Streams/Queue.cs`
  - `ISourceQueue<in T>`
  - `Task<IQueueOfferResult> OfferAsync(T element)`
  - `ISourceQueueWithComplete<in T> : ISourceQueue<T>`

Implementation:

- `src/core/Akka.Streams/Implementation/Sources.cs`
  - `QueueSource<TOut>`
  - `QueueSource<TOut>.Offer<T>`
  - `QueueSource<TOut>.Logic`
  - `QueueSource<TOut>.Materialized.OfferAsync`

Tests:

- `src/core/Akka.Streams.Tests/Dsl/QueueSourceSpec.cs`

API approval snapshots:

- `src/core/Akka.API.Tests/verify/CoreAPISpec.ApproveStreams.Net.verified.txt`
- `src/core/Akka.API.Tests/verify/CoreAPISpec.ApproveStreams.DotNet.verified.txt`

## Current Mechanics

`Materialized.OfferAsync` creates a `TaskCompletionSource<IQueueOfferResult>`, sends an `Offer<TOut>` into the graph logic callback, and returns the task.

The offer completes when:

- downstream demand arrives and the element is pushed,
- the stream closes and pending offers receive `QueueClosed`,
- overflow strategy returns `Dropped` / `Failure`,
- a second backpressure offer is rejected with `IllegalStateException`,
- the stage stops and pending callback offers fail with `StreamDetachedException`.

Under `OverflowStrategy.Backpressure`, if the buffer is full, the first extra offer becomes `_pendingOffer`. That task intentionally stays incomplete until downstream demand frees space.

## Key Design Constraint

Cancelling the returned task is not enough. If the stage still holds `_pendingOffer`, a later downstream pull can still push the cancelled offer's element into the stream. A correct implementation needs cancellation to also remove or invalidate the pending offer inside the graph logic.

Therefore, a pure caller-side wrapper around `OfferAsync(element).WithCancellation(token)` would not be correct for this issue.

## Recommended V1 Shape

Add a `CancellationToken` overload to `ISourceQueueWithComplete<T>`, not just a timeout overload:

```csharp
Task<IQueueOfferResult> OfferAsync(T element, CancellationToken cancellationToken);
```

Reasons:

- Matches .NET async API conventions.
- Allows callers to use deadlines, request aborts, and actor/application cancellation policies.
- Timeout can be layered by callers with `CancellationTokenSource.CancelAfter`.
- Keeps API surface smaller than adding both token and timeout.
- Avoids changing the smaller `ISourceQueue<T>` interface when the DSL surface for `Source.Queue<T>` already materializes `ISourceQueueWithComplete<T>`.

Compatibility:

- Additive public API.
- Existing inherited `OfferAsync(T element)` remains unchanged.
- `QueueSource<TOut>.Materialized` can share implementation between the existing overload and the new cancellable overload.

## Implementation Sketch

1. Add `using System.Threading;` to `Queue.cs` and `Sources.cs`.
2. Add `OfferAsync(T element, CancellationToken cancellationToken)` to `ISourceQueueWithComplete<T>`.
3. Add `NonBlockingTrySetCanceled<T>` to `TaskEx`, alongside `NonBlockingTrySetResult` and `NonBlockingTrySetException`.
4. Use the `QueueSource<TOut>.Offer<T>` instance itself as the offer identity.
5. In `Materialized.OfferAsync(element, cancellationToken)`:
   - if `cancellationToken.IsCancellationRequested`, return a canceled task before invoking the graph logic,
   - create non-blocking `TaskCompletionSource<IQueueOfferResult>`,
   - construct the `Offer<TOut>` instance,
   - invoke the normal offer before registering cancellation,
   - register the token to invoke a new graph input that cancels this specific offer by reference,
   - dispose the registration when the offer task completes,
   - use this order to preserve FIFO behavior through `GetAsyncCallback`.
6. Add a new internal graph input, for example `CancelOffer`, carrying the target `Offer<TOut>` reference and token.
7. In graph logic:
   - if `CancelOffer` matches `_pendingOffer`, clear `_pendingOffer` and cancel the offer task,
   - if cancellation arrives after the offer already completed, ignore it,
   - if `_terminating` was waiting only for the pending offer, complete the stage after clearing it,
   - ensure a canceled offer is not pushed later.
8. Keep buffer behavior unchanged for offers that are immediately enqueued.

Important race window:

- Token already canceled at call entry: return a canceled task and do not invoke graph logic.
- Token fires after the offer was queued through `_invokeLogic` but before stage logic processes it: rely on FIFO ordering by invoking `Offer` before registering the cancellation callback; `CancelOffer` will be processed after the offer.
- Token fires after the offer became `_pendingOffer`: `CancelOffer` clears the pending slot.
- Token fires after the offer completed: no-op.

## Test Plan

Add tests to `QueueSourceSpec`:

1. Backpressured pending offer completes as canceled when its token is canceled.
2. A canceled pending offer is not emitted after downstream later requests.
3. A canceled pending offer frees the queue so a later offer can become pending/enqueued normally.
4. Already-canceled token returns a canceled task and does not enqueue.
5. Cancellation after successful enqueue does not alter an already completed result.
6. Bufferless `maxBuffer == 0` pending offer can be canceled and is not emitted later.
7. Cancellation after `Complete()` while an offer is pending has deterministic behavior and does not leave the stage stuck.

Run:

```bash
dotnet test src/core/Akka.Streams.Tests/Akka.Streams.Tests.csproj --filter "FullyQualifiedName~QueueSourceSpec"
dotnet test src/core/Akka.API.Tests/Akka.API.Tests.csproj
```

If public API approval snapshots fail, update only the relevant approved/verified API files after inspecting the diff.

## Questions Before Coding

1. Should cancellation complete the offer task as canceled, or should it return a queue-specific `IQueueOfferResult`?
   - Recommended: canceled task, because the caller supplied `CancellationToken`.
2. Should the overload live on `ISourceQueue<T>` or only on `ISourceQueueWithComplete<T>`?
   - Recommended: `ISourceQueueWithComplete<T>`, because `Source.Queue<T>` materializes that interface and this limits blast radius for third-party `ISourceQueue<T>` implementors.
3. Should a timeout overload be added too?
   - Recommended: no for v1; users can use `CancellationTokenSource.CancelAfter`.
4. Should cancellation complete the task with the original token?
   - Recommended: yes, via `TrySetCanceled(cancellationToken)`.

## Current Assessment

This is a good candidate for a small but meaningful PR. The main risk is getting cancellation semantics correct inside the graph stage, not the public API addition itself.
