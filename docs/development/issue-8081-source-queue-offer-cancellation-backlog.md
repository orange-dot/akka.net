# Implementation Backlog: Issue 8081 — `Source.Queue` `OfferAsync` Cancellation

Date: 2026-04-28

Companions: `issue-8081-source-queue-offer-cancellation-plan.md`,
`issue-8081-source-queue-offer-cancellation-angle.md`

Branch: `fix/8081-source-queue-offer-cancellation` (local only)

## Context

Issue 8081 in akka.net describes that `ISourceQueue<T>.OfferAsync(element)`
under `OverflowStrategy.Backpressure` can hang indefinitely when downstream
demand never arrives. A caller-side `Task.WithCancellation(token)` is unsafe
because the stage still owns `_pendingOffer` and the next `OnPull` will push
that element after the caller has moved on.

The fix is a token-aware overload that cooperates with the graph stage.
Design has been pinned in the plan and angle docs:

- new `OfferAsync(element, cancellationToken)` lives on the opt-in
  `ICancellableSourceQueueWithComplete<T>` derived interface, not as a
  required member on existing `ISourceQueueWithComplete<T>` or
  `ISourceQueue<T>`;
- `ISourceQueueWithComplete<T>` gets an extension overload that delegates to
  `ICancellableSourceQueueWithComplete<T>` only when the materialized queue
  supports cancellation-aware offers;
- no `TimeSpan` overload in v1; callers use `CancellationTokenSource.CancelAfter`;
- no new `IQueueOfferResult.Cancelled` variant; cancellation completes the
  task via `TrySetCanceled(token)`;
- offer identity is the `Offer<TOut>` instance itself (reference equality);
- a new `CancelOffer` `IInput` is added inside `QueueSource<TOut>`;
- ordering inside the new overload: invoke `Offer` first, register the
  cancel callback second — FIFO of the async-callback indirection then
  guarantees the stage sees `Offer` before any `CancelOffer`;
- `_terminating` re-check fires when cancellation removes the last pending
  offer, mirroring `OnPull` / `OnDownstreamFinish`;
- test coverage spans both buffered and bufferless (`maxBuffer == 0`) paths.

This backlog turns those decisions into atomic, sequenced units of work.

## Branch & Target

- Branch: `fix/8081-source-queue-offer-cancellation` (local only)
- PR target: `dev` (per repo `CLAUDE.md`)
- All file paths below are repo-relative.

## Backlog

Each item names files, the change, and an acceptance check. Items are
ordered for execution; dependencies are marked.

### Phase A — Preparation

**B1. Add `NonBlockingTrySetCanceled<T>` extension to `TaskEx`.**
- File: `src/core/Akka/Util/Internal/TaskEx.cs`
- Match the style of existing `NonBlockingTrySetException<T>`:
  ```csharp
  public static void NonBlockingTrySetCanceled<T>(
      this TaskCompletionSource<T> taskCompletionSource,
      CancellationToken cancellationToken)
      => taskCompletionSource.TrySetCanceled(cancellationToken);
  ```
- The "non-blocking" name is preserved for symmetry; the call delegates
  directly because `TrySetCanceled` already runs continuations
  synchronously on the calling thread the same way the other helpers do.
- Acceptance: file builds; no public API surface change (internal namespace).

### Phase B — Stage core

**B2. Add `internal sealed class CancelOffer : IInput` inside `QueueSource<TOut>`.**
- File: `src/core/Akka.Streams/Implementation/Sources.cs`
- Add `using System.Threading;` if not already present.
- Shape:
  ```csharp
  internal sealed class CancelOffer : IInput
  {
      public CancelOffer(Offer<TOut> target, CancellationToken token)
      {
          Target = target;
          Token = token;
      }
      public Offer<TOut> Target { get; }
      public CancellationToken Token { get; }
  }
  ```
- Place near `Offer<T>` / `Completion` / `Failure` declarations.
- Acceptance: file builds; type is internal-only.

**B3. Add `CancelOffer` arm to `Logic.Callback()`.**
- File: `src/core/Akka.Streams/Implementation/Sources.cs`
- Inside the `GetAsyncCallback<IInput>` body, after the `Failure` arm:
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
      // Otherwise: offer already completed (Enqueued / Dropped / QueueClosed
      // / IllegalStateException) or never made it to pending — drop silently.
  }
  ```
- Depends on B1 (helper) and B2 (input type).
- Acceptance: existing tests still pass; behavior of non-cancellation paths
  unchanged (no edits to other arms).

### Phase C — Materializer surface

**B4. Add `Materialized.OfferAsync(TOut, CancellationToken)`.**
- File: `src/core/Akka.Streams/Implementation/Sources.cs`
- New method body (Strategy 1: invoke Offer, then Register):
  ```csharp
  public Task<IQueueOfferResult> OfferAsync(TOut element, CancellationToken cancellationToken)
  {
      if (cancellationToken.IsCancellationRequested)
          return Task.FromCanceled<IQueueOfferResult>(cancellationToken);

      var promise = TaskEx.NonBlockingTaskCompletionSource<IQueueOfferResult>();
      var offer = new Offer<TOut>(element, promise);
      _invokeLogic(offer);

      if (cancellationToken.CanBeCanceled)
      {
          var registration = cancellationToken.Register(
              () => _invokeLogic(new CancelOffer(offer, cancellationToken)));
          promise.Task.ContinueWith(
              static (_, state) => ((CancellationTokenRegistration)state!).Dispose(),
              registration,
              TaskContinuationOptions.ExecuteSynchronously);
      }

      return promise.Task;
  }
  ```
- The `cancellationToken.CanBeCanceled` guard skips the registration and
  continuation when the token is `CancellationToken.None`, so the no-token
  path stays allocation-free beyond what the existing overload allocates.
- Depends on B2, B3.
- Acceptance: file builds; no behavior change for callers of the existing
  `OfferAsync(TOut)` overload.

**B5. Rewrite existing `Materialized.OfferAsync(TOut)` as delegate.**
- File: `src/core/Akka.Streams/Implementation/Sources.cs`
- New body:
  ```csharp
  public Task<IQueueOfferResult> OfferAsync(TOut element)
      => OfferAsync(element, CancellationToken.None);
  ```
- The `cancellationToken.CanBeCanceled` guard inside B4 makes this a
  zero-allocation, two-extra-branch path for non-token callers
  (`CancellationToken.None.CanBeCanceled == false` short-circuits the
  registration and continuation). One source of truth, no duplicated
  TCS plumbing.
- Depends on B4.
- Acceptance: existing tests calling the no-token overload still pass
  byte-for-byte semantically.

### Phase D — Public interface

**B6. Add the new method to `ISourceQueueWithComplete<in T>`.**
- File: `src/core/Akka.Streams/Queue.cs`
- Add `using System.Threading;` if not already present.
- Add to the interface (with full xmldoc, no `TBD`):
  ```csharp
  /// <summary>
  /// Cancellable variant of <see cref="ISourceQueue{T}.OfferAsync(T)"/>.
  /// Cancelling <paramref name="cancellationToken"/> while the offer is
  /// pending under <see cref="OverflowStrategy.Backpressure"/> removes
  /// the pending offer from the stage and completes the returned task
  /// as canceled. If the offer has already completed (Enqueued, Dropped,
  /// QueueClosed, or IllegalStateException) the cancellation is a no-op.
  /// Awaiters observe <see cref="OperationCanceledException"/> carrying
  /// the supplied token.
  /// </summary>
  Task<IQueueOfferResult> OfferAsync(T element, CancellationToken cancellationToken);
  ```
- Note: `ISourceQueue<T>` is **not** modified. This is deliberate.
- Depends on B4 (so the materializer already implements it).
- Acceptance: file builds; `Materialized` already satisfies the contract
  via B4.

### Phase E — Public API approval

**B7. Regenerate the API approval snapshots.**
- Run from repo root:
  ```bash
  dotnet test src/core/Akka.API.Tests/Akka.API.Tests.csproj -c Release
  ```
- Test will fail; inspect the diff between `received` and `verified` files.
- Expected diff scope: addition of one new method line on
  `ISourceQueueWithComplete<in T>` (around line 723 of both verified files)
  and on `QueueSource<TOut>.Materialized` (around line 3558). No other
  changes.
- If diff matches expectations, copy received over verified for both:
  - `src/core/Akka.API.Tests/verify/CoreAPISpec.ApproveStreams.Net.verified.txt`
  - `src/core/Akka.API.Tests/verify/CoreAPISpec.ApproveStreams.DotNet.verified.txt`
- Re-run the test; it must pass.
- Depends on B4, B6.

### Phase F — Tests

All tests live in `src/core/Akka.Streams.Tests/Dsl/QueueSourceSpec.cs`.
Class base is `AkkaSpec`; materialization pattern uses
`Source.Queue<int>(...).ToMaterialized(this.SinkProbe<int>(), Keep.Both).Run(_materializer)`.
Place new tests next to the existing `Backpressure` tests around line 72.

**B8. Test — buffered backpressure: pending offer cancels.**
- Setup: `Source.Queue<int>(1, OverflowStrategy.Backpressure)`, fill the buffer,
  issue a second offer with a `CancellationTokenSource` token.
- Cancel the token before downstream pulls.
- Assert: returned task ends `Canceled` with `OperationCanceledException`
  carrying the same token.
- Assert: downstream subsequently requests an element and only the
  originally-buffered element arrives — the canceled element is never pushed.

**B9. Test — bufferless backpressure: pending offer cancels.**
- Setup: `Source.Queue<int>(0, OverflowStrategy.Backpressure)` (no buffer;
  `_pendingOffer` is the rendez-vous slot).
- First offer becomes pending immediately. Cancel its token.
- Assert: task canceled; element not pushed when downstream later pulls.

**B10. Test — cancel frees the slot for the next offer.**
- After B8's setup, cancel the pending offer; then issue a new offer with
  a fresh token.
- Assert: the new offer succeeds (Enqueued or pending normally) — i.e.
  the canceled pending did not leave the stage in a wedged state.

**B11. Test — pre-canceled token short-circuits.**
- Cancel the token before calling `OfferAsync`.
- Assert: returned task is already `Canceled`.
- Assert: downstream sees no element from this call (verify by sending
  a separate uncanceled offer and observing only that one).

**B12. Test — cancel after success is no-op.**
- Issue an offer that immediately enqueues (small buffer with room).
- Await result, assert `Enqueued`.
- Then cancel the token.
- Assert: result task remains `RanToCompletion` with `Enqueued`.

**B13. Test — cancel after `QueueClosed` deterministic.**
- Make an offer pending under backpressure; call `Complete()` on the queue
  (which may not drain the pending immediately) and then cancel.
- Per `OnDownstreamFinish` / `Completion` arm semantics, the pending offer
  should complete with `QueueClosed`, not canceled, because `Complete()` was
  issued first.
- Assert: result task is `RanToCompletion` with `QueueClosed`. Cancellation
  is a no-op.
- Note: this codifies the order-of-events answer ("first event wins").

**B14. Test — cancel during the async-callback queueing window (window B).**
- Required for v1 PR.
- Goal: exercise the gap between `_invokeLogic(offer)` enqueueing the
  `Offer` input and the stage actually processing it on the materializer
  dispatcher. The test must show that even when cancel fires during that
  gap, the offer is never pushed and the result task ends canceled.
- Implementation approach (preferred): hold the materializer dispatcher
  busy for a controllable window using a backpressuring map stage that
  blocks on a `ManualResetEventSlim`, then:
  1. Materialize `Source.Queue<int>(0, OverflowStrategy.Backpressure)`
     into a sink behind the blocking stage.
  2. Issue `OfferAsync(element, ct)` from the test thread; the offer
     enters the dispatcher queue but cannot be processed yet.
  3. From the test thread, cancel `ct`. The cancellation registration
     fires on the test thread and posts `CancelOffer` behind the still-
     queued `Offer` (FIFO).
  4. Release the blocking stage.
  5. Await the offer task — it must end canceled.
  6. Assert downstream sees zero elements.
- Fallback approach (if a controllable dispatcher harness is not
  available): a tight loop of N (e.g. 1000) offer+cancel pairs where
  cancel runs on a separate `Task.Run` immediately after `OfferAsync`
  returns. Assert: across all iterations, every canceled task ends in
  one of `Canceled` or a successful `IQueueOfferResult` (Enqueued etc.),
  never both pushed and canceled, and the queue never wedges.
- Document in the test method's xmldoc which timing this exercises and
  why it complements B8/B9/B11 (those cover the post-pending and
  pre-stage paths; B14 covers the in-flight gap).

### Phase G — Docs and release notes

**B15. RELEASE_NOTES.md entry.**
- File: `RELEASE_NOTES.md`
- Append under the existing `#### 1.6.0 August 15th, 2025 ####` placeholder
  block (or whatever section the project considers the next-version
  drafting target — confirm conventions in surrounding entries).
- Wording (draft):
  > **Akka.Streams**: added `ISourceQueueWithComplete<T>.OfferAsync(T element, CancellationToken cancellationToken)`.
  > Cancelling the token while the offer is backpressured removes the
  > pending offer from the stage and completes the returned task as
  > canceled (`OperationCanceledException` carrying the token). The
  > existing `OfferAsync(T element)` overload is unchanged. Resolves #8081.

**B16. XML doc audit.**
- Verify all newly-added members carry full xmldoc, no `TBD` placeholders
  (the project has been actively replacing `TBD`s, e.g. commit `d9e5a62d7`).
- Files touched: `Queue.cs` (B6), `Sources.cs` (B2, B4).

### Phase H — Local validation

**B17. Build clean and warnings-as-errors.**
```bash
dotnet build -c Release
dotnet build -warnaserror
```

**B18. Targeted test run.**
```bash
dotnet test src/core/Akka.Streams.Tests/Akka.Streams.Tests.csproj \
    --filter "FullyQualifiedName~QueueSourceSpec" -c Release
```

**B19. API approval test passes.**
```bash
dotnet test src/core/Akka.API.Tests/Akka.API.Tests.csproj -c Release
```

**B20. Format check.**
```bash
dotnet format --verify-no-changes
```

### Phase I — Commit and push

**B21. Logical commits on `fix/8081-source-queue-offer-cancellation`.**
Suggested split:
- **(a)** Stage core: B1 helper + B2 input + B3 callback arm.
- **(b)** Materializer surface: B4 new overload + B6 interface method + B16 xmldoc.
- **(c)** API snapshots: B7 verified file updates.
- **(d)** Tests: B8–B14.
- **(e)** Release notes: B15.

Each commit message references issue 8081. Commits do not skip hooks
(no `--no-verify`).

**B22. Push branch to user's fork.**
- `git push -u origin fix/8081-source-queue-offer-cancellation`
- Confirm with user before pushing (per session policy on hard-to-reverse
  / externally visible actions).

**B23. Open draft PR against `dev`.**
- PR title: `fix(streams): add cancellation token overload to ISourceQueueWithComplete<T>.OfferAsync (#8081)`
- PR body covers: bug summary, why a caller-side wrapper is unsafe,
  chosen API placement (`ISourceQueueWithComplete<T>` only, not
  `ISourceQueue<T>`), cancellation semantics, FIFO ordering note,
  links to plan + angle docs in `docs/development/`.
- Mark as draft until reviewers explicitly approve readiness.

## Critical Files Modified

- `src/core/Akka/Util/Internal/TaskEx.cs` — B1
- `src/core/Akka.Streams/Implementation/Sources.cs` — B2, B3, B4, B5
- `src/core/Akka.Streams/Queue.cs` — B6
- `src/core/Akka.API.Tests/verify/CoreAPISpec.ApproveStreams.Net.verified.txt` — B7
- `src/core/Akka.API.Tests/verify/CoreAPISpec.ApproveStreams.DotNet.verified.txt` — B7
- `src/core/Akka.Streams.Tests/Dsl/QueueSourceSpec.cs` — B8–B14
- `RELEASE_NOTES.md` — B15

## Reused Existing Utilities

- `TaskEx.NonBlockingTaskCompletionSource<T>` — already in
  `src/core/Akka/Util/Internal/TaskEx.cs`; used by B4.
- `TaskEx.NonBlockingTrySetResult<T>` / `NonBlockingTrySetException<T>` —
  pattern model for the new `NonBlockingTrySetCanceled<T>` (B1).
- `GetAsyncCallback<TInput>` graph-stage indirection — already used by
  `QueueSource<TOut>.Logic` for `Offer` / `Completion` / `Failure`; B3
  rides the same channel for `CancelOffer`.
- `QueueSourceSpec` `AkkaSpec` + `SinkProbe<T>` materialization pattern —
  reused for B8–B14 (no new test infrastructure).

## Verification (end-to-end)

Run from repo root, in order:

1. `dotnet build -c Release` — clean compile.
2. `dotnet build -warnaserror` — warnings clean.
3. `dotnet test src/core/Akka.Streams.Tests/Akka.Streams.Tests.csproj --filter "FullyQualifiedName~QueueSourceSpec" -c Release` — all `QueueSourceSpec` tests green, including the seven new ones (B8–B14).
4. `dotnet test src/core/Akka.API.Tests/Akka.API.Tests.csproj -c Release` — API approval green after the verified-file update.
5. `dotnet format --verify-no-changes` — style clean.

If all five pass, the branch is ready for push and draft PR.

## Out of Scope for v1

- `TimeSpan` overload (callers use `CancellationTokenSource.CancelAfter`).
- New `IQueueOfferResult.Cancelled` variant.
- Adding cancellation to `ISourceQueue<T>` directly.
- Touching other overflow-strategy paths.
- Anything in `BoundedSourceQueue` or sink queues — separate work.

## Notes for the Executor

- Backlog ends at "draft PR opened." Reviewer-feedback iteration is
  intentionally outside this backlog and handled ad hoc.
- Read this together with the plan and angle docs in the same folder:
  the plan captures pinned design decisions, the angle captures the
  reasoning trail behind those decisions, and this backlog is the
  execution path.
