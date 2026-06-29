# Issue 8081: Angle on the Cancellation Plan

Date: 2026-04-28

Companion to: `issue-8081-source-queue-offer-cancellation-plan.md`

Branch: `fix/8081-source-queue-offer-cancellation` (local, not pushed)

Audience: future-self / co-author / upstream PR reviewer.

## Posture

The plan's central insight is correct and load-bearing: a caller-side
wrapper such as `OfferAsync(element).WithCancellation(token)` is **not** a
fix. It cancels the *Task*, not the *offer*. Under
`OverflowStrategy.Backpressure`, the stage still owns `_pendingOffer`, and
the next `OnPull` will push that element downstream. The real fix lives
inside `QueueSource<TOut>.Logic`, not in the public API surface.

Everything below treats that as settled and pushes on the parts the plan
either understates or leaves open.

Review adjustment: the original Option A still breaks downstream
implementers because it adds a required member to an existing public
interface. The compatible shape is an opt-in derived interface,
`ICancellableSourceQueueWithComplete<T>`, implemented by `Source.Queue`'s
materialized value, plus an extension overload on
`ISourceQueueWithComplete<T>` that delegates only when that capability is
present.

## Source Reading That Anchors The Angle

The relevant code is `src/core/Akka.Streams/Implementation/Sources.cs`.
Three observations matter for the design:

1. There is exactly **one** slot for an unresolved promise:
   `_pendingOffer`. Once an offer is enqueued into `_buffer`, the promise
   has already completed with `Enqueued`; the buffer holds elements, not
   offers. So the "cancel target" is always either `_pendingOffer` or
   nothing.
2. The bufferless path (`maxBuffer == 0`, `_buffer is null`) also uses
   `_pendingOffer` as a one-slot rendez-vous — see lines 259–265 of
   `Sources.cs`. Cancellation must work there too. It is easy to overlook
   because the `Backpressure` discussion focuses on the buffered case.
3. `Materialized.OfferAsync` posts an `Offer<TOut>` through
   `_invokeLogic`, which is the `GetAsyncCallback<IInput>` indirection on
   the stage. The stage processes inputs serially on the materializer
   thread. Cancellation has to ride the same channel, or it races with
   `OnPull`.

## The Race Window The Plan Understates

The plan says cancellation must remove `_pendingOffer` inside graph logic.
True. But there are actually **three** distinct timings, and the design
must answer all three:

| Timing | State of the offer | Required behavior |
| ------ | ------------------ | ----------------- |
| A. Token already canceled at call entry | Offer not yet sent to stage | Return canceled task, do not invoke `_invokeLogic` |
| B. Cancel fires while offer is queued in the async-callback indirection but stage has not yet processed it | Offer is in flight; not yet `_pendingOffer` | Stage must recognize the offer arriving as already-canceled and not push it |
| C. Cancel fires after the offer became `_pendingOffer` | Offer is the pending slot | Stage must clear `_pendingOffer` and complete its TCS as canceled |
| D. Cancel fires after the offer already completed (Enqueued / Dropped / Failure / QueueClosed / IllegalStateException) | Promise is in a terminal state | No-op |

The plan covers C and D explicitly, A implicitly, and **misses B**. B is
a real window because `GetAsyncCallback` queues the input; the stage may
not run for many microseconds. A token registration created on the
caller's thread can absolutely fire in that window.

Two viable strategies for B:

- **Strategy 1 — order operations and rely on FIFO.**
  In `OfferAsync(element, ct)`:
  1. If `ct.IsCancellationRequested`, return canceled task immediately.
  2. Create the promise.
  3. `_invokeLogic(new Offer<TOut>(element, promise, identity))`.
  4. `var reg = ct.Register(() => _invokeLogic(new CancelOffer(identity)));`
  5. On promise completion, dispose `reg`.

  Because step 3 enqueues the Offer before step 4 enqueues a possible
  CancelOffer, and `GetAsyncCallback` is FIFO per dispatcher, the Offer
  always arrives at the stage first. The stage's CancelOffer handler can
  then assume the offer is already represented in `_pendingOffer` (or has
  already completed) — but **not** that the offer is still pending.

  Risk: `CancellationToken.Register` invokes the callback synchronously
  on the registering thread *if the token is already canceled*. The
  step-1 fast-path check covers that, but a token canceled between
  step 1 and step 4 will fire synchronously inside `Register`. That is
  fine — Offer is already enqueued; CancelOffer enqueues right after;
  FIFO holds.

- **Strategy 2 — give the stage a small "preemptively canceled" set.**
  The stage tracks a set (or single slot, since at most one offer is
  unresolved at a time per queue) of identities that were canceled
  before processing. If CancelOffer arrives before its Offer, the
  identity goes into the set. When the Offer arrives, the stage checks
  the set and short-circuits to a canceled completion.

  This is more robust to any future change in queueing semantics. It
  costs one extra reference field on `Logic`.

I recommend **Strategy 1** for v1 because it is smaller, exploits the
existing FIFO guarantee already used by Completion / Failure inputs, and
needs no new state on `Logic`. Strategy 2 is the right answer if the
team later introduces input coalescing or any non-FIFO dispatch on the
async-callback indirection.

## Stage-Side Specifics

`CancelOffer` should be a new internal `IInput` carrying just an offer
identity. Identity by **reference equality on the `Offer<TOut>`
instance** is sufficient and simpler than a monotonic id:

- The caller already holds a reference to the `Offer<TOut>` it
  constructed. Capturing it in the `Token.Register` closure is the
  natural shape.
- `_pendingOffer == cancelInput.Target` is a single ref-eq check.
- No id allocation, no overflow, no need for thread-safe counters.

The `CancelOffer` handler in `Logic.Callback()`:

```csharp
if (input is CancelOffer cancel)
{
    if (_pendingOffer != null && ReferenceEquals(_pendingOffer, cancel.Target))
    {
        _pendingOffer.CompletionSource.NonBlockingTrySetCanceled(cancel.Token);
        _pendingOffer = null;
        if (_terminating)
        {
            _completion.SetResult(new object());
            CompleteStage();
        }
    }
    // else: offer already enqueued / dropped / closed — nothing to do.
}
```

Note the `_terminating` check: it mirrors `OnPull` and `OnDownstreamFinish`
because canceling the last pending offer can be the event that allows a
pending `Complete()` to finish the stage.

`NonBlockingTrySetCanceled` does not exist yet on the project's
`TaskEx` helpers — check `src/core/Akka.Streams/Util/TaskEx.cs` and add
it alongside `NonBlockingTrySetResult` / `NonBlockingTrySetException`.
The non-blocking variant exists to avoid running TCS continuations
inline on the stage thread; cancellation has the exact same hazard.

## Public API Surface — Where The Method Should Live

The plan proposes adding `OfferAsync(T, CancellationToken)` to
`ISourceQueue<T>`. Two facts make me push back on that exact placement:

1. The materialized value of `Source.Queue<T>(...)` is
   `ISourceQueueWithComplete<T>`, not `ISourceQueue<T>`. Look at
   `CoreAPISpec.ApproveStreams.Net.verified.txt` line 2065:
   ```
   public static Akka.Streams.Dsl.Source<T,
       Akka.Streams.ISourceQueueWithComplete<T>>
       Queue<T>(int bufferSize, Akka.Streams.OverflowStrategy overflowStrategy)
   ```
   Every user that gets cancellation through this issue holds an
   `ISourceQueueWithComplete<T>`. They never hold a bare
   `ISourceQueue<T>` from this DSL.
2. Adding an abstract method to **either** public interface is a
   binary-breaking change for any third-party implementor. The
   project's CLAUDE.md "extend-only" rule is about not changing existing
   methods; adding new abstract methods is permitted but the
   `Akka.API.Tests` snapshot will flag it, which forces an explicit
   review step. That is fine — but it means we should pick *one*
   interface and carry the cost there, not both.

Three placement options, in order of how I'd rank them:

**Option A — add only on `ISourceQueueWithComplete<T>`.**

```csharp
public interface ISourceQueueWithComplete<in T> : ISourceQueue<T>
{
    Task<IQueueOfferResult> OfferAsync(T element, CancellationToken cancellationToken);
    // … existing members …
}
```

Pros: aligns with where users actually hold the queue. Leaves
`ISourceQueue<T>` untouched, which protects the smaller more general
interface from churn. Only one verified-API snapshot diff.

Cons: any future queue source that materializes as bare `ISourceQueue<T>`
won't have cancellation. None exists in tree today; if one is added,
this can be revisited.

**Option B — add a new sub-interface
`ISourceQueueWithCancellation<T> : ISourceQueue<T>`.**

`Materialized` implements both. Existing return type stays
`ISourceQueueWithComplete<T>`; users who want cancellation cast or are
served via a new factory. Heavier. JVM Akka has analogous fragmentation
(`BoundedSourceQueue`), so there is precedent, but it complicates the
DSL surface.

**Option C — add on `ISourceQueue<T>`.**

Plan's current proposal. Most general, most disruptive to external
implementors of the small interface (e.g. test doubles, custom queue
sources outside the tree). I'd avoid this unless the team explicitly
wants to make cancellation a queue-wide contract.

I'd go with **Option A**.

## Result Semantics

`TrySetCanceled(cancellationToken)` is the right completion. Awaiters
get `OperationCanceledException(token)`, which is what every `async`
method that takes a `CancellationToken` produces. Do **not** invent a
new `IQueueOfferResult.Cancelled` variant — adding a new result enum
forces every existing switch on `IQueueOfferResult` in user code to add
a case, which is a real source-breaking change in C#'s exhaustiveness
landscape (especially with `nullable` and analyzers). The canceled task
is the cleaner contract.

If a result-shaped variant is wanted later, it can be layered on top
without changing the cancellation behavior of the task.

## JVM Parity

CLAUDE.md says "Maintain compatibility with JVM Akka while being .NET
idiomatic." JVM Akka's `Source.queue.offer(elem)` returns a Java
`CompletionStage` and does not take a `CancellationToken` because Java
does not have one. A `CancellationToken` overload is the .NET idiomatic
shape and does not contradict JVM parity — it adds a feature that the
JVM API would have to express through a different mechanism anyway. The
PR description should make this explicit so reviewers do not flag it as
divergence.

## Test Plan Gaps

The plan lists five tests. All five are necessary; I would add four
more to nail the corners:

6. **`maxBuffer == 0` (bufferless) path with backpressure.**
   The plan implicitly assumes the buffered path. The bufferless path
   uses `_pendingOffer` as a rendez-vous slot (see `Sources.cs`
   lines 259–265). A pending offer there must also be cancelable.

7. **Cancel before stage processes the offer (window B above).**
   Hand-construct a scenario where the materializer thread is
   blocked / starved long enough that a cancellation registration
   fires before the stage drains its callback queue. Easiest way:
   use a `TestScheduler` or a single-thread dispatcher and an actor
   that holds the dispatcher busy for one turn. The offer must end up
   canceled and never pushed.

8. **Cancel races with `OnPull`.**
   Schedule cancellation and a downstream pull such that they arrive
   at the stage in opposite orders across runs. Both interleavings
   must be safe: either the offer is pushed (Enqueued result, late
   cancel is no-op) or it is canceled (no push). Never both.

9. **Cancel after `Complete()` was issued but before stage drained.**
   The pending offer must complete with **either** `QueueClosed`
   **or** canceled, deterministically — pick one and document it.
   I'd argue `QueueClosed` wins because `Complete()` was issued
   first, but the implementation should make that explicit.

Plus the existing five — keep all of them; tests 1–5 are the
"obvious" cases and the spec needs them as documentation of intent.

## Implementation Order I'd Suggest

1. Add `NonBlockingTrySetCanceled` to `TaskEx`. Tiny prep.
2. Extend `Offer<T>` with no new fields. Identity is reference equality.
3. Add `internal sealed class CancelOffer : IInput` carrying
   `Offer<TOut>` (the target) and `CancellationToken` (so the
   completion records the originating token).
4. Add the `CancelOffer` arm to `Logic.Callback()` as sketched above,
   including the `_terminating` re-check.
5. Add `Task<IQueueOfferResult> OfferAsync(TOut element, CancellationToken cancellationToken)` to `Materialized`.
   Order: fast-path canceled return, build promise, invoke Offer,
   register cancel callback, dispose registration on promise
   completion via a `ContinueWith(_ => reg.Dispose(),
   TaskContinuationOptions.ExecuteSynchronously |
   TaskContinuationOptions.OnlyOnRanToCompletion |
   TaskContinuationOptions.OnlyOnCanceled |
   TaskContinuationOptions.OnlyOnFaulted)`. (Combine the Only*
   flags as appropriate; the cleanest is a single
   `ContinueWith(t => reg.Dispose())` with no flags, since
   `Dispose` is idempotent.)
6. Add the new method to `ISourceQueueWithComplete<T>` (Option A).
7. Update `CoreAPISpec.ApproveStreams.Net.verified.txt` and
   `CoreAPISpec.ApproveStreams.DotNet.verified.txt` after running the
   API tests and inspecting the diff.
8. Tests in `QueueSourceSpec` per the merged list.
9. RELEASE_NOTES.md entry under the next release.
10. Verify XML doc on the new overload is present and not `TBD` —
    the project has been actively replacing `TBD` placeholders
    (commit d9e5a62d7), so don't add new ones.

## Branch & PR Mechanics

- The repo's PR target is `dev`, per CLAUDE.md. Confirm
  `fix/8081-source-queue-offer-cancellation` is currently rebased on
  `dev` (it is, top of `dev` is `e7366a038`).
- Do **not** push until at least the public-API surface is locked,
  because the verified-snapshot diff is what reviewers will read first.
- The PR description should:
  - state the bug in plain terms (offer task can hang, and a wrapper
    cancellation is unsafe),
  - explain why the fix is inside the stage,
  - call out Option A and the deliberate non-touch of `ISourceQueue<T>`,
  - link to this angle doc and the plan doc.

## Open Questions Before Coding

These deserve an answer in writing before the patch lands, ideally on
the issue thread so reviewers do not relitigate:

- **Q1.** Does Akka.NET have any external implementor of
  `ISourceQueueWithComplete<T>` that we know of? If yes, the new
  abstract method is a breaking change for them — call it out in
  RELEASE_NOTES. (My read: implementors of `ISourceQueueWithComplete`
  outside the tree are vanishingly rare; it is consumed, not
  implemented.)
- **Q2.** For Strategy 1, is there any code path in
  `GetAsyncCallback` that can reorder inputs across threads? I do
  not believe so, but a one-line confirmation in the PR ("graph
  stage async callback is FIFO per stage") would close it.
- **Q3.** Is `OperationCanceledException(token)` the right shape for
  awaiters, or do callers expect a `TaskCanceledException`? Both
  derive from `OperationCanceledException`; `TrySetCanceled(token)`
  produces the former. .NET convention since .NET 6 is the former.
  I'd document that explicitly in xmldoc.

## What Not To Do

- Do not add a `TimeSpan` overload alongside the token overload in
  v1. `CancellationTokenSource.CancelAfter` covers it without
  expanding the API surface.
- Do not change `_pendingOffer` to a queue or list. It is a
  one-slot invariant the rest of the stage relies on; cancellation
  fits into that invariant cleanly.
- Do not introduce a new dispatcher or dedicated cancellation
  thread. The async-callback indirection is the right and only
  channel for state changes inside this stage.
- Do not adjust unrelated overflow strategies "while we're here."
  Keep the patch scoped to cancellation.

## Summary

The plan has the right shape. Three reinforcements to bake in before
coding starts: (1) treat the pre-stage cancellation race as a
first-class case, not an afterthought; (2) place the new method on
`ISourceQueueWithComplete<T>`, not `ISourceQueue<T>`, because that is
where the materialized value already lives; (3) extend the test plan
to cover the bufferless path and the timing windows that are easy to
hand-wave away. Identity by reference equality on `Offer<TOut>` is
enough; no monotonic id is needed. Cancellation completes via a
canceled task, not a new result variant. Patch stays small.
