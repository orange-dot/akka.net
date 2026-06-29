# Issue 5381: Angle on `MergeSubstreams` Returning `IFlow` — and Its Theoretical Foundations

Date: 2026-06-13

Branch: `fix/5381-merge-substreams-return-type` (local, not pushed)

Base: `origin/dev@5c7485eec` ("Unify agent instructions into AGENTS.md (CLAUDE.md symlink) (#8261)")

Audience: future-self / co-author / upstream PR reviewer.

Scope of this note: a complete, read-only analysis of akkadotnet/akka.net
issue **#5381** — the reading of the original bug and the comment thread, the
source evidence that anchors it, the design options for a fix, the project
constraints any fix runs into, and finally the **type-theoretic foundations**
of why this problem exists at all. Nothing in the working tree was modified to
produce this; it is a design/analysis record.

---

## 1. The issue, verbatim in substance

Reporter: **Aaronontheweb**, opened 2021-11-11, against Akka.NET v1.4.28,
module Akka.Streams. Labeled `up for grabs`, `potential bug`, `akka-streams`.

Repro (abbreviated):

```csharp
var source = Source
    .Cycle(() => Enumerable.Range(1, 100).Cast<long>().GetEnumerator())
    .WireTapMaterialized(FlowRateMonitor("outbound/s"), Keep.None)
    .Async()
    .GroupBy(10, l => l % 10)
    .Sum((l, l1) => l + l1)
    .MergeSubstreams()
    .WireTapMaterialized(FlowRateMonitor("outbound/s"), Keep.None) // <-- will not bind for an external user
```

Stated problem: `MergeSubstreams` returns `IFlow<TIn, TMat>` rather than a
`Flow<TIn, TMat>`, "which breaks our ability to continue to use the
Akka.Streams builder interface to work with its output."

- Expected: returns `Flow<TIn, TMat>`
- Actual: returns `IFlow<TIn, TMat>`

### The thread

- **ismaelhamed (2022-06-13)** posted a "this would work" snippet that chains
  `.MergeSubstreams().WireTapMaterialized(...)` and then casts the whole chain
  back to `Source<long, Task<Done>>` for `RunWith`.
- **leonardo-lurci-deltatre / ltouro (2024-07)** asked where
  `WireTapMaterialized` comes from (answer: `FlowOperations.cs#L1909` upstream),
  then reported: *"Unfortunately it does not work because `.WireTapMaterialised`
  is an extension method for `Flow<TIn, TOut, TMat>` but `.MergeSubstreams`
  returns `IFlow<TOut, TMat>`. Am I doing something wrong?"*

The thread therefore contains an apparent contradiction: a maintainer says it
works, an external user says the same shape does not compile. Section 5
resolves that contradiction precisely.

---

## 2. Source reading that anchors the angle

All references are against `origin/dev@5c7485eec`, paths under
`src/core/Akka.Streams/`.

### The type that carries the bug

`Dsl/SubFlow.cs:21` declares the abstract class with **three** type
parameters:

```csharp
public abstract class SubFlow<TOut, TMat, TClosed> : IFlow<TOut, TMat>
```

Two families of "exit" methods live on it, and they are typed asymmetrically:

| Method | Location | Return type |
| ------ | -------- | ----------- |
| `To<TMat2>(sink)` (close the scope) | `SubFlow.cs:71` | `TClosed` |
| `MergeSubstreams()` | `SubFlow.cs:79` | `IFlow<TOut, TMat>` |
| `MergeSubstreamsWithParallelism(int)` | `SubFlow.cs:91` (abstract) | `IFlow<TOut, TMat>` |
| `ConcatSubstream()` | `SubFlow.cs:101` | `IFlow<TOut, TMat>` |

The **closed** exit is polymorphic in the root (`TClosed`); the **merged-back**
exit is hard-coded to the weak `IFlow`. That asymmetry *is* the bug. See
Section 3 (`TClosed` works) and the whole of the theory part (why `F[_]` cannot).

The two convenience methods reduce to the abstract one, and the abstract one is
implemented in `Implementation/SubFlowImpl.cs:124`:

```csharp
public override IFlow<TOut, TMat> MergeSubstreamsWithParallelism(int parallelism)
    => _mergeBackFunction.Apply(Flow, parallelism);
```

`SubFlow.cs:79` / `:101`:

```csharp
public virtual IFlow<TOut, TMat> MergeSubstreams()      => MergeSubstreamsWithParallelism(int.MaxValue);
public virtual IFlow<TOut, TMat> ConcatSubstream()      => MergeSubstreamsWithParallelism(1);
```

(Hold onto `int.MaxValue` vs `1` — Section 7.3 shows they are two different
monad multiplications.)

### What `TClosed` resolves to at the public boundary

- `Dsl/SourceOperations.cs:1221` — `Source.GroupBy` returns
  `SubFlow<TOut, TMat, IRunnableGraph<TMat>>`.
- `Dsl/FlowOperations.cs:1353` — `Flow.GroupBy` returns
  `SubFlow<TOut, TMat, Sink<TIn, TMat>>`.

So `To` correctly yields `IRunnableGraph<TMat>` for a Source-rooted chain and
`Sink<TIn, TMat>` for a Flow-rooted chain. The closed side is already
root-polymorphic. The merged side is not.

### The merge-back machinery

`Implementation/SubFlowImpl.cs:18-28` — the indirection that produces the
merged stream:

```csharp
public interface IMergeBack<TIn, TMat>
{
    IFlow<TOut, TMat> Apply<TOut>(Flow<TIn, TOut, TMat> flow, int breadth);
}
```

`Dsl/Internal/InternalFlowOperations.cs:1402-1407` — the GroupBy realization:

```csharp
public IFlow<T, TMat> Apply<T>(Flow<TOut, T, TMat> flow, int breadth) =>
    _self.Via(new Fusing.GroupBy<TOut, TKey>(_maxSubstreams, _groupingFunc, _allowClosedSubstreamRecreation))
         .Select(f => f.Via(flow))
         .Via(new Fusing.FlattenMerge<Source<T, NotUsed>, T, NotUsed>(breadth));
```

`_self` is the original `Source`/`Flow`. `_self.Via(...)` on a `Source`
**returns a `Source` at runtime**. That single fact (Section 3) downgrades the
bug from "semantic" to "static-only."

### The materialized-value boundary

`Implementation/SubFlowImpl.cs:81-96` and `:106-109` — `ViaMaterialized`,
`MapMaterializedValue`, and `RunWith` all `throw new NotImplementedException()`.
This is deliberate and matches the class doc on `SubFlow.cs:14-17`: substreams
cannot contribute to the super-flow's materialized value. The fix must respect
this (Section 9).

### The `WireTapMaterialized` surface the reporter actually hits

- `Dsl/SourceOperations.cs:1824-1825` — public `Source.WireTapMaterialized`
  returns `Source<TOut, TMat3>` and is implemented by **casting** the result of
  the internal `IFlow` operation:

  ```csharp
  public static Source<TOut, TMat3> WireTapMaterialized<...>(this Source<TOut, TMat> flow, ...) =>
      (Source<TOut, TMat3>)InternalFlowOperations.WireTapMaterialized(flow, that, materializerFunction);
  ```

- `Dsl/FlowOperations.cs:2009-2010` — same shape for `Flow.WireTapMaterialized`,
  casting to `Flow<TIn, TOut, TMat3>`.
- `Dsl/Internal/InternalFlowOperations.cs:2527` — the underlying
  `WireTapMaterialized(this IFlow<TOut, TMat> ...)` returning `IFlow<TOut, TMat3>`,
  defined inside `internal static class InternalFlowOperations`
  (`InternalFlowOperations.cs:29`).

Two things fall out of this: (a) the public fluent surface is typed for
`Source`/`Flow`, not `IFlow`; (b) the codebase **already** uses the
"call the `IFlow` op, cast the result back to the concrete type" idiom. The fix
is an application of an existing idiom, not new design.

---

## 3. The bug is compile-time only — and that is provable

`GroupByMergeBack.Apply` (above) returns `_self.Via(...)`. `Via` on a `Source`
yields a `Source`; on a `Flow` yields a `Flow`. So the object handed back from
`MergeSubstreams()` **already is** a concrete `Source<TOut, TMat>` /
`Flow<TIn, TOut, TMat>` at runtime — only its *static* type has been widened to
`IFlow<TOut, TMat>`.

Consequences:

- The 2022 downcast "works" because the runtime witness is genuinely the
  concrete type; the cast is sound, not a hack that happens to pass.
- No graph is impossible to run. The defect is purely about the static surface
  the DSL exposes — i.e. developer experience and type safety, not stream
  semantics. (This is the corrected framing of the prior "graph can run / it's
  only API" intuition: here it is grounded in the exact line that produces the
  value.)

---

## 4. Reconciling 2022 ("works") vs 2024 ("does not compile")

The `internal` modifier is the smoking gun.

- `InternalFlowOperations` is **`internal static class`**
  (`InternalFlowOperations.cs:29`) and contains
  `WireTapMaterialized(this IFlow<TOut, TMat> ...)` (`:2527`).
- The public `Source`/`Flow` overloads are thin wrappers that *cast* that
  internal result (`SourceOperations.cs:1824`, `FlowOperations.cs:2009`).

Therefore:

- **ismaelhamed (in-repo, internals visible):** after `MergeSubstreams()` the
  static type is `IFlow`; his `.WireTapMaterialized` binds to the *internal*
  `IFlow` extension; a final cast recovers `Source` for `RunWith`. "This would
  work" — but only because the internal API is visible in his compilation
  context.
- **leonardo (external user):** the internal extension is invisible; only the
  `Source`/`Flow` public overloads exist; neither matches an `IFlow` receiver;
  the chain does not compile. "Unfortunately it does not work."

Same source text, different visibility. The earlier "ismael just cast it"
reading is incomplete: a cast alone would not make `.WireTapMaterialized` bind
on an `IFlow` receiver for an external user — the missing piece is that the only
`IFlow`-typed fluent surface is **internal**. (One stated assumption: ismael's
snippet ran where internals are visible, which holds for maintainer test code.)

---

## 5. The angle, distilled

Agreement with the obvious reading of the issue:

- The symptom (returns `IFlow`, too weak) is real.
- It is an API/DX defect, not a runtime-semantics bug.
- A fix must cover all three of `MergeSubstreams`,
  `MergeSubstreamsWithParallelism`, and `ConcatSubstream`.
- It must be polymorphic in the root: `Source.GroupBy(...).MergeSubstreams()`
  must return `Source`, `Flow.GroupBy(...).MergeSubstreams()` must return
  `Flow`.

Sharpenings / corrections:

1. **The issue title's "Expected: `Flow`" is itself imprecise.** The repro is
   rooted in a `Source`, so the faithful expectation there is
   `Source<long, TMat>`, not `Flow`. A naive "just return `Flow`" fix would be
   wrong for the Source case. This forces a root-polymorphic fix.
2. **Name the root cause:** C# lacks higher-kinded types; the `IFlow`
   degradation is the shadow of that. See Section 7.
3. **The 2022/2024 split is about `internal` visibility,** not merely casting
   (Section 4).
4. **There is no purely additive (binary-compatible) fix** (Section 8).

---

## 6. Fix options

1. **Source/Flow-specific `SubFlow` subtypes (recommended).** Introduce
   `SourceSubFlow<TOut, TMat> : SubFlow<TOut, TMat, IRunnableGraph<TMat>>` and
   `FlowSubFlow<TIn, TOut, TMat> : SubFlow<TOut, TMat, Sink<TIn, TMat>>`, each
   refining `MergeSubstreams` / `MergeSubstreamsWithParallelism` /
   `ConcatSubstream` to the concrete return type via a **covariant return**
   (C# 9+), implemented by casting the existing `IFlow` result — the same idiom
   already used at `SourceOperations.cs:1825`. Change the public `GroupBy` /
   `SplitWhen` / `SplitAfter` return types to these subtypes. Smallest blast
   radius that is still correct. This is, theoretically, a **manual
   monomorphization** of the closed world `{Source, Flow}` (Section 8).
2. **A fourth type parameter on `SubFlow`** (the faithful port of Scala's
   `F[_]`). Conceptually cleanest, but changes the **arity** of `SubFlow<,,>`,
   breaking every reference to that generic name in the public API. Not worth it
   versus (1).
3. **`WireTapMaterialized` (and friends) overloads on `IFlow` — reject.** Only
   additive, but wrong: it pollutes `IFlow` with the whole fluent surface, does
   not restore the Source-vs-Flow distinction, and would require adding *every*
   chainable operator. Extension methods cannot help here anyway, since the
   instance method `SubFlow.MergeSubstreams()` shadows any extension in overload
   resolution.

---

## 7. Theoretical foundations

This is the part worth keeping. The bug is the precise shadow of one boundary in
type theory.

### 7.1 What the type must express

Abstractly:

```
mergeSubstreams : SubFlow[Out, Mat, F] -> F[Out]
```

where `F` is the root that produced the SubFlow:

- Source root: `F[X] = Source[X, Mat]` (Mat fixed)
- Flow root:   `F[X] = Flow[In, X, Mat]` (In, Mat fixed)

`F` is **not a type**; it is a **type constructor**, `F : * -> *`. For
`mergeSubstreams` to be polymorphic in the root, the language must permit a
**type parameter that is itself a constructor** — higher-kinded polymorphism
(HKT). That is exactly where C# stops.

### 7.2 Kinds — the actual boundary

In a kinded type system (System Fω), types are classified by *kinds*:

- `*` — proper types (`int`, `Source<int, Mat>`)
- `* -> *` — unary constructors (`List`, `Option`, our `F`)
- `* -> * -> *` — binary constructors (`Source` as a two-place constructor)

**C# generics admit only kind-`*` parameters.** `T` in `Foo<T>` can be
instantiated only by a proper type; there is no syntax and no kind system for
`where F : (* -> *)` followed by use of `F<int>` inside. Scala has it
explicitly: `SubFlow[+Out, +Mat, +F[+_], C]` — the `[+_]` declares `F` of kind
`* -> *` (with covariance).

Precise placement:

> C#'s type system is approximately **System F with subtyping, restricted to
> kind `*`** (plus reified generics, bounded and F-bounded quantification, and
> interface/delegate-only variance). Scala is approximately **Fω with
> subtyping** (DOT calculus). The bug lives exactly in the gap `F` vs `Fω` — the
> presence/absence of the arrow `->` in the language of kinds.

`mergeSubstreams : F[Out]` requires Fω; it is therefore inexpressible in C#, and
the port degraded it to `IFlow`.

### 7.3 Why `TClosed` survives but `F[_]` cannot

The asymmetry of Section 2 has a one-line cause:

- **`C` is an already-applied type — kind `*`.** Closing the scope with a sink
  *consumes* the element type; on the closed path `Out` no longer varies, so the
  result (`IRunnableGraph<Mat>` / `Sink<In, Mat>`) is a ground type at the call
  site. Kind `*` → C# carries it as an ordinary parameter `TClosed`.
- **`F` must stay an un-applied constructor — kind `* -> *`.** On the open path
  `Out` is still live and keeps changing with every downstream operator. The
  return must be `F` applied to the *current* `Out`. Since `Out` varies, `F`
  cannot be pre-applied; you genuinely need it as an un-applied `* -> *` thing.

> The closed type is **consumed** (ground, kind `*`); the open type is still
> **productive** (must abstract over the live element type, kind `* -> *`). The
> asymmetry is exactly where kind-`*`-only generics stop being expressive
> enough.

### 7.4 Existential vs universal quantification — the heart

Writing `SubFlow<TOut, TMat, TClosed>` and dropping `F` **existentially
quantifies** the root: "there exists some `F` that produced me, but I have
forgotten which." `IFlow<TOut, TMat>` is essentially the existential package
`∃F. F[Out, Mat]`.

An existential lets you **use** the hidden type through its interface; it does
not let you **reconstruct** it. To return `F[Out]`, `F` must be **universally**
available — a parameter the caller chose — i.e. it must never have been packed.
Scala keeps `F[_]` universal; Akka.NET packed it prematurely.

The whole solution taxonomy is "what you do with the witness of the hidden
type":

- **Downcast (the 2022 workaround)** = *unsafe existential unpacking*: assert at
  runtime "the witness is `Source`." Works because the witness physically
  survived.
- **HKT (`F[_]`)** = *never pack; keep `F` universal.*
- **CRTP / F-bounds** = *partial witness retention* via a self-referential bound
  (Section 7.5).

And the subtle, important closure — is the information *lost* or merely
*hidden*?

> Existential packing **hides**; it does not **destroy**. The witness exists in
> the package. C# additionally **reifies** generics, so the object is really an
> instance of `Source\`2`. The information is not gone — only statically
> inaccessible. The `IFlow` degradation is **conservative**: it preserves
> semantics and kills only ergonomics. That is precisely why the unsafe downcast
> is **sound** — it unpacks an existential whose witness genuinely matches.

This reframes the defect: not a loss of information, but a *premature hiding* of
information that physically never leaves.

### 7.5 F-bounded polymorphism — C#'s strongest tool, one kind too short

The reflex for "return my own concrete type" in C#/Java is F-bounded
polymorphism (= CRTP in C++; the same family as builders, `clone`, fluent APIs,
self-types):

```csharp
interface ISelf<T> where T : ISelf<T>
```

This threads the concrete type as a parameter bounded by an interface that names
itself — a self-reference of **kind `*`**. It solves "return `Source<Out, Mat>`"
**only when `Out` is fixed**.

But `merge` *changes the element type* (after `groupBy().Select().merge`, the
output is a new type). Returning `Source<NEW, Mat>` requires **applying the
self-constructor to a fresh argument** — which is `F[_]`, HKT again.

> Even the cleverest C# trick falls **exactly one rung up the kind ladder**
> short: F-bounds pin a *type*; you need to pin a *constructor* and then apply
> it to a new element. Swift's `Self` and Scala's `this.type` are stronger than
> F-bounds but still kind `*` — they would not solve the element-changing return
> either.

### 7.6 Variance — the second, smaller gap

Scala writes `+Out`, `+F[+_]` (covariant), `C` invariant. C# **class** type
parameters are always invariant; variance (`in`/`out`) exists only on interfaces
and delegates. `SubFlow` is an abstract *class*, so `TOut/TMat/TClosed` are all
invariant. Even the `IFlow<TOut, TMat>` fallback therefore cannot be covariant
in `TOut` the way Scala's `Out` is — you lose subtyping on the merged stream
too. A smaller gap beneath the HKT one, but part of the complete picture.

### 7.7 The categorical layer — why the return *must* be `F[Out]`

Why is the HKT requirement forced rather than incidental? Look again at
`GroupByMergeBack.Apply` (`InternalFlowOperations.cs:1402-1407`):

```
F[X] --groupBy--> F[F[X]] --Select(via inner)--> F[F[Y]] --FlattenMerge--> F[Y]
```

That is `map` then `join` = `flatMap`. Specifically:

- `groupBy` is the `F[X] -> F[F[X]]` direction (a "stream of streams").
- `mergeSubstreams` is the **monad multiplication** `μ : F[F[Out]] -> F[Out]`
  (`join` / `flatten`).

The codomain of `μ` is, **by definition**, `F` applied to `Out`. You cannot type
`join` without naming the functor `F`; naming an un-applied functor is HKT.

> The HKT requirement is **forced by the categorical signature of `join`**, not a
> stylistic choice.

Better still, the two convenience methods are two **different** multiplications
on the same functor:

- `ConcatSubstream() => MergeSubstreamsWithParallelism(1)` (`SubFlow.cs:101`) —
  the *ordered / sequential* join (list-like, properly associative → a lawful
  monad).
- `MergeSubstreams() => MergeSubstreamsWithParallelism(int.MaxValue)`
  (`SubFlow.cs:79`) — the *concurrent* join (associative only up to
  nondeterministic interleaving).
- `parallelism = n` **interpolates** between the two — the parameter selects
  which multiplication.

(Caveat: monad laws hold firmly for the *concat* join; for the *merge* join they
hold only up to interleaving. So "lawful monad" for concat, "monad-like" for
merge.)

### 7.8 Solutions, in theory

1. **Native HKT** (Scala/Haskell) — the `F[_]` parameter. Unavailable in C#.
2. **Defunctionalized HKT encoding** (Reynolds defunctionalization; concretely
   Yallop & White 2014, *"Lightweight higher-kinded polymorphism"*, FLOPS).
   Reify the application `F[X]` as a nominal proxy `App<TBrand, X>` plus
   hand-written coercions `Inj : F<X> -> App<TBrand, X>` and `Prj` back.
   Recovers HKT *expressiveness* at the cost of: per-constructor boilerplate,
   **unchecked** coercions at the brand boundary (you trust the encoding, not the
   kind-checker), and poor ergonomics. It exists in .NET (LanguageExt `K<F, A>`,
   the "Higher" library). Theoretically possible for Akka.NET; practically
   unacceptable for an idiomatic-C# library — which is exactly why the port chose
   the `IFlow` degradation instead.
3. **Manual monomorphization (the recommended fix).** Since you cannot quantify
   *over* the constructor, **enumerate** the constructors by hand:
   `SourceSubFlow`, `FlowSubFlow`. This is monomorphization performed by the
   programmer rather than the compiler. It loses open extensibility (a third root
   would need a third hand-written subtype) but covers the closed world
   `{Source, Flow}`. The enabling language feature is **covariant return types**
   (C# 9, 2020): an `override` returning `Source` where the base returns `IFlow`,
   valid because `Source : IFlow`. Before C# 9 you would fall back to
   `new`-method hiding, which dispatches statically and leaks through the base
   type — so the *language version* is part of the story.

### 7.9 Thesis, in one sentence

> `mergeSubstreams` is the monad multiplication `μ : F[F[Out]] -> F[Out]`; its
> codomain forces naming the un-applied functor `F` (kind `* -> *`); C# generics
> live at kind `*`; so the port had to existentially pack `F` into `IFlow`,
> which did not *destroy* information (the reified witness survives at runtime,
> which is why the downcast is sound) but only *statically hid* it — and the fix
> is a manual monomorphization of the two roots with a covariant return.

---

## 8. Project constraints any real fix runs into

These are imposed by the repo's own `AGENTS.md` and matter for the PR shape.

- Any correct fix **changes a public return type** (`GroupBy` / `SplitWhen` /
  `SplitAfter`, and/or `SubFlow`'s own methods). That is **source-compatible**
  (`var sub = src.GroupBy(...)` still compiles; `SubFlow<...> sub = ...` still
  compiles because the new subtype is assignable) but **binary-breaking** (the
  return type is part of the method signature in IL).
- Therefore:
  - `Akka.API.Tests` approval files
    (`src/core/Akka.API.Tests/CoreAPISpec.ApproveCore.approved.txt` and
    siblings) **must** be regenerated/approved.
  - A `BREAKING_CHANGES_V1.6.md` entry is **mandatory in the same PR**
    (status / component / type / change / migration).
  - It is **not** back-portable to `v1.4`; it is a v1.6-cycle change.
- "Just add overloads" understates this. Frame it accurately:
  **source-compatible, binary-breaking, v1.6-only, with API-approval churn.**

---

## 9. Materialized-value caveat and the test to add

`SubFlowImpl.ViaMaterialized` / `MapMaterializedValue` / `RunWith` throw
`NotImplementedException` by design (`SubFlowImpl.cs:81-96`, `:106-109`) —
substreams do not contribute to the super-flow's materialized value. The fix
must preserve `TMat` across the merge, which the current `Apply` already does
(`_self.Via(...)` keeps `TMat`; only the inner `FlattenMerge<..., NotUsed>` is
`NotUsed`).

Test to land with the fix — the exact reporter scenario, which is also the only
one that proves the DX is restored:

```csharp
// must compile with NO cast, and Keep.Right materialization must survive:
source.GroupBy(...).Sum(...).MergeSubstreams()
      .WireTapMaterialized(sink, Keep.Right)
```

Assert both that it compiles against the public surface (no
`Akka.Streams.Dsl.Internal` import) and that the materialized value of the
wire-tap sink is the one observed.

---

## 10. Open question worth discussing

Because the degradation is **conservative** (the witness survives, so the
downcast is sound), one can defend `IFlow` as a deliberate trade-off rather than
a bug. The real decision is then **ergonomics vs. closed-world purity**:

- enumerate `Source`/`Flow` (lose any third root, gain compile-time fluency), or
- keep the cast (retain openness, lose type safety at the seam).

That is, at bottom, a choice between **universal** and **existential**
quantification at the level of API design, not merely implementation. The
recommendation in Section 6 (option 1) takes the universal/closed-world side,
which is the right call for a mainstream .NET library where the fluent builder
experience is the product.

## 11. Implemented — both directions, as two PRs (A/B comparison)

Both fixes were built and validated on the fork (`origin` = `orange-dot/akka.net`,
never upstream), each off `origin/dev@5c7485eec`, as two independent draft PRs.
This section records what they cost in practice and confirms the theory empirically.

Environment note that unblocked approach A: `Akka.Streams` now single-targets
**net10.0** with **C# 12** (`Directory.Build.props` LangVersion 12.0), so
**covariant return types are available** (an older net48/netstandard target would
have blocked them — that runtime lacks `RuntimeFeature.CovariantReturnsOfClasses`).
Both `Source` and `Flow` implement `IFlow<TOut,TMat>` (`Source.cs:38`, `Flow.cs:29`),
so `override IFlow → Source/Flow` is legal.

### Approach B — `MergeBack` extension (PR `claude-5381-subflow-mergeback-ext`, #4)

- New `Dsl/SubFlowMergeBackOperations.cs`: two `MergeBack(parallelism = int.MaxValue)`
  overloads keyed on `TClosed` (`IRunnableGraph<TMat>` → `Source`,
  `Sink<TIn,TMat>` → `Flow`). Same name; overload resolution separates them because
  the two `TClosed` shapes never unify.
- Leverages the Section 7.3 fact directly: `TClosed` already discriminates the root
  **and** survives all 75 operators (it is a threaded parameter independent of the
  changing element type), so `GroupBy(...).Sum(...).MergeBack()` binds to the concrete
  type. The downcast is sound by Section 3.
- **Additive, non-breaking** — no `BREAKING_CHANGES_V1.6.md` entry; back-portable.
- Validation: `SubFlowMergeBackSpec` 4/4 (compile-time proof via explicitly typed
  locals + runtime, across GroupBy/SplitWhen/parallelism); Streams API approval +2.

### Approach A — typed `SourceSubFlow`/`FlowSubFlow` (PR `claude-5381-subflow-typed-subtypes`, #5)

- New abstract `SourceSubFlow<TOut,TMat>` / `FlowSubFlow<TIn,TOut,TMat>` with
  **covariant** `MergeSubstreams*` overrides returning the concrete type; new
  `SourceSubFlowImpl`/`FlowSubFlowImpl` **decorators** that wrap the base substream and
  re-wrap on `Via` so the subtype survives operator chaining (the decorator is what
  makes Section 5/6's "operator erosion" not happen).
- 14 `Source`/`Flow` entry points (`GroupBy`/`SplitWhen`/`SplitAfter`) retyped to return
  the subtypes; `Source/FlowSubFlowOperations` mirror the operator surface — **69
  operators each**, generated by `CodeGen/gen_subflow_ops.py` (the codegen route from
  Section 7.8, not hand-typed).
- **Honest omission proven by the compiler:** 6 materialized-value operator overloads
  (`AlsoToMaterialized`, `DivertToMaterialized`, `WatchTermination`,
  `InterleaveMaterialized`, `MergeMaterialized`) are **not** mirrored — they return
  `SubFlow<TOut, TMat3, TClosed>` (a *changed* mat type), which the fixed-`TClosed`
  subtype cannot represent, and they already throw on substreams via `ViaMaterialized`.
  They fall through to base `SubFlowOperations`. This is exactly the Section 7.3
  boundary surfacing again: anything whose result type depends on a varying type
  parameter resists the kind-`*` subtype encoding.
- **Source-compatible, binary-breaking** on the 14 return types; `BREAKING_CHANGES_V1.6.md`
  entry added; Streams API approval +208/−14.
- Validation: `SubFlowTypedReturnSpec` 4/4 + 47 existing substream specs pass;
  `FlowGroupBySpec` updated to drop a now-unnecessary `((Source<...>)source)` cast;
  build clean (0 warnings).

### Side by side

| | B — `MergeBack` | A — typed subtypes |
| --- | --- | --- |
| Files | 2 src + test | 13 (6 src + 2 impl + 2×69 ops + test + generator) |
| Diff | +~70 / 0 | +1194 / −42 |
| Breaking | none (additive) | binary (14 return types) |
| Back-port to v1.4 | yes | no (v1.6-only) |
| `.MergeSubstreams()` itself | still `IFlow` | concrete `Source`/`Flow` |
| Operator coverage | all (via `TClosed`) | 69/75 (6 mat-changing omitted) |
| Maintenance | trivial | one generator vs. 3-place drift |
| PR (draft, fork) | `orange-dot/akka.net#4` | `orange-dot/akka.net#5` |

### Verdict

The experiment confirms the theory. Approach **B** buys ~90% of the value
(the real chain `GroupBy().Sum().MergeBack()` returns the concrete type) at ~5% of
the cost, precisely because `TClosed` is a ground (kind `*`) witness that threads
through operators for free — the one piece of the Scala `Repr[+_]` machinery that
*does* survive C#'s kind ceiling. Approach **A** restores the literal
`.MergeSubstreams()` name but costs a manual monomorphization of the entire operator
surface (codegen-assisted) plus a binary break, and still cannot type the
mat-changing operators — the residue of the missing higher-kinded type. B is the one
to ship; A stands as the faithful demonstration of what "doing it properly" costs
without HKT.
