# Issue #8081 — Pregled dve varijante: opt-in interfejs vs. direktno dodavanje na `ISourceQueueWithComplete<T>`

**Repo:** `workspace/platform/akka.net`
**Baza:** `upstream/dev` @ `e7366a038`
**Varijanta A:** `fix/8081-source-queue-offer-cancellation` @ `86b275af8` — *opt-in*, novi `ICancellableSourceQueueWithComplete<T>` + extension metoda na `ISourceQueueWithComplete<T>`
**Varijanta B:** `fix/8081-source-queue-direct-api` @ `d430a6035` — direktno dodavanje `OfferAsync(T, CancellationToken)` na `ISourceQueueWithComplete<T>`
**Pregled obavljen:** 2026-05-06
**Autor zahteva:** Aaron Stannard (Lightbend, maintainer); labele: `akka-streams`, `api-change`, `up for grabs`, `good for first-time contributors`

---

## 0. Sažetak (TL;DR)

| Aspekt | Varijanta A (opt-in) | Varijanta B (direct) |
|---|---|---|
| Bukvalno ispunjava tekst issue-a | Ne (novi interfejs + extension) | **Da** |
| Breaking change za eksterne implementere `ISourceQueueWithComplete<T>` | **Ne** | **Da** (hard-break na netstandard2.0) |
| Poštuje `CLAUDE.md` *"Extend-only design - don't modify existing public APIs"* | **Da** | **Ne** |
| Linije promene (bez testova) | ~133 | ~90 |
| Pokrivenost defensive grana ekstenzije | Delimična (ne-Cancelable, null sourceQueue, NotSupported nisu testirani) | n/a (nema ekstenzije) |
| Ergonomija pozivaoca (po pozivu nakon `Source.Queue<T>`) | `source.OfferAsync(elem, ct)` radi (extension), ne treba downcast | `source.OfferAsync(elem, ct)` radi direktno |
| ApiApprovals diff (DotNet) | 11 linija (uključuje novi tip + extension) | 4 linije (samo nova metoda na interfejsu + na `Materialized`) |
| Test bazen | identično 213 dodatnih linija, 7 novih `[Fact]`, sve prolaze | identično 213 dodatnih linija, 7 novih `[Fact]`, sve prolaze |
| Verifikacija lokalno (`net10.0`) | `QueueSourceSpec` 33/33; `Akka.API.Tests` 18/18 | `QueueSourceSpec` 33/33; `Akka.API.Tests` 18/18 |

**Preporuka:** **Varijanta A** je ispravniji upstream PR za `dev` granu Akka.NET-a (pravac `1.5.x`) jer ne lomi javni interfejs i ima isti runtime-efekat. **Varijanta B** je bolja samo ako maintainer-i izričito žele da iskoriste sledeći major (`2.0.x`) za clean-cut izmenu — ostaje kao backup, ali ne treba je predlagati u prvom navratu PR-a. Detaljno obrazloženje ispod.

---

## 1. Verifikacija stanja repoa

Sve tvrđene SHA cifre su pronađene lokalno:

```
upstream/dev:                                    e7366a0389157d78993f0222a83248ed076fd45b
fix/8081-source-queue-offer-cancellation (A):    86b275af85b036dcc34cb54a02becce991cb9388
fix/8081-source-queue-direct-api         (B):    d430a6035a7f9234057b6143bba307edf061dc3f
```

Diff `--stat` (poređenje sa `upstream/dev`):

| Fajl | A | B |
|---|---:|---:|
| `RELEASE_NOTES.md` | +1 | +1 |
| `CoreAPISpec.ApproveStreams.DotNet.verified.txt` | +11 | +4 |
| `CoreAPISpec.ApproveStreams.Net.verified.txt` | +11 | +4 |
| `Dsl/QueueSourceSpec.cs` | +213 | +213 |
| `Implementation/Sources.cs` | +61 | +59 |
| `Streams/Queue.cs` | +61 | +14 |
| `Util/Internal/TaskEx.cs` | +11 | +11 |
| ukupno (insertions/deletions) | 365 / 4 | 303 / 3 |

`A vs B` diff (`fix/8081-source-queue-offer-cancellation..fix/8081-source-queue-direct-api`):
- `Queue.cs`: B uklanja extension klasu i dodatni interfejs (47 linija manje), umesto toga ubacuje 1 metodu u postojeći interfejs.
- `Sources.cs`: B menja samo deklaraciju `Materialized` da implementira `ISourceQueueWithComplete<TOut>` (ne `ICancellableSourceQueueWithComplete<TOut>`).
- `RELEASE_NOTES.md`: B navodi "interface method"; A navodi i interface+extension.
- `*.verified.txt`: očekivano se razlikuju jer B mutira postojeći interfejs.

**Test fajlovi su identični u obe grane** (potvrđeno `git diff fix/...A...fix/...B... -- src/core/Akka.Streams.Tests/Dsl/QueueSourceSpec.cs` daje prazan diff). Implementacija u `Sources.cs` je takođe **funkcionalno identična** — semantika cancellation-a je ista u obe grane; razlikuje se isključivo tip kojem `Materialized` deklariše implementaciju.

---

## 2. Nezavisno pokrenuti testovi

Pokrenuto pojedinačno na obe grane, framework `net10.0`, `Release`:

```bash
env DOTNET_CLI_HOME=/tmp/dotnet-home DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1 \
  dotnet test src/core/Akka.Streams.Tests/Akka.Streams.Tests.csproj \
    --filter FullyQualifiedName~QueueSourceSpec -c Release --framework net10.0 -v minimal
```

| Grana | Passed | Failed | Skipped | Total | Trajanje |
|---|---:|---:|---:|---:|---:|
| A `fix/8081-source-queue-offer-cancellation` | 33 | 0 | 0 | 33 | 7s |
| B `fix/8081-source-queue-direct-api`         | 33 | 0 | 0 | 33 | 8s |

ApiApprovals (`Akka.API.Tests`):

| Grana | Passed | Failed | Skipped | Total |
|---|---:|---:|---:|---:|
| A | 18 | 0 | 0 | 18 |
| B | 18 | 0 | 0 | 18 |

**Nije pronađen nijedan `Skip(...)`, `Trait("Skip"...)`, `[Fact(Skip=...)]`, niti `#if false` u dodatim testovima.** Helperi `AssertSuccess` (sa `task.Wait(3s).Should().BeTrue()`) i `AssertCanceledWithToken` (sa `AwaitAssertAsync(...3s, 50ms...)` + `Assert.ThrowsAnyAsync<OperationCanceledException>` + `exception.CancellationToken.Should().Be(cancellationToken)`) su prave provere stanja, ne dekorativne.

`grep` po `TODO|FIXME|HACK|XXX|Thread\.Sleep|async void` u dijfu obe grane vraća **nijedan** novi pogodak. Jedine pojave `.Result` i `.Wait()` su unutar postojećeg `AssertSuccess` helpera (kojeg Aaron koristi u upstream-u već godinama), tj. nije dodato u ovoj patch grupi.

---

## 3. Odgovori na 5 pitanja

### 3.1 Pitanje 1 — Da li Varijanta A dovoljno zadovoljava issue #8081 iako overload nije direktan member `ISourceQueueWithComplete<T>`, nego extension + `ICancellableSourceQueueWithComplete<T>`?

**Da, za 95% praktičnih korisnika — ali nije bukvalno onako kako issue traži.**

Issue kaže (citat):
> Please add one of the following to `ISourceQueueWithComplete<T>`:
> 1. `Task<IQueueOfferResult> OfferAsync(T element, CancellationToken cancellationToken)`
> 2. A timeout-aware overload (for example `OfferAsync(T element, TimeSpan timeout)`)

Tekst je literalno *"add ... to ISourceQueueWithComplete\<T\>"*. Varijanta A to **ne radi direktno** — dodaje izvedeni interfejs `ICancellableSourceQueueWithComplete<T>` i extension metodu sa istim potpisom `OfferAsync(T, CancellationToken)`.

Ali šta korisnik vidi na pozivnoj strani:

```csharp
var source = Source.Queue<int>(0, OverflowStrategy.Backpressure)
    .To(Sink...).Run(_materializer);

// Varijanta A: rezolucija ide na extension, jer Materialized JESTE ICancellableSourceQueueWithComplete<int>
var task = source.OfferAsync(42, cts.Token);

// Varijanta B: rezolucija ide na sam interfejs
var task = source.OfferAsync(42, cts.Token);
```

Identičan poziv, identičan runtime efekat. Korisnik **ne vidi razliku** dok god njegov queue dolazi iz `Source.Queue<T>`.

Razlika je vidljiva samo u tri scenarija:

1. **Reflektivno otkrivanje API-ja** (npr. introspekcija interfejsa za generisanje proxy-ja, mock-ova). Varijanta A interface ne sadrži novu metodu, pa će proxy generator propustiti.
2. **Eksterno definisani `ISourceQueueWithComplete<T>`** koji se ručno dispatchuje u Akka pipe — ako neko ima tipiziran wrapper pa pokuša da prosledi `CancellationToken`, dobiće `NotSupportedException` iz extension metode, ne tihi fallback. To je *bolje* od tihog ignorisanja, ali je promena u runtime ponašanju za korisnike koji su ranije polimorfno koristili tip kroz wrapper-e.
3. **Default implementacija u DIM-u** — nije moguća jer `Akka.Streams` cilja `netstandard2.0` (vidi sekciju 3.3); ali bi pomogla B varijanti.

**Rezime za pitanje 1:** A *jeste* funkcionalno zadovoljavajuća. Sa tačke gledišta tipičnog korisnika `Source.Queue<T>` koji upravo to konstruira i poziva `OfferAsync(elem, ct)` direktno, A je pun ekvivalent B. Ono što A *ne* radi jeste da napravi `OfferAsync(T, CancellationToken)` prvoklasnim članom `ISourceQueueWithComplete<T>`. Issue je formalno naznačio jednu od dve opcije; A je *treća, srednja* opcija koju autor issue-a nije eksplicitno tražio ali koja je u Akka.NET tradiciji *"Extend-only design"* (vidi `CLAUDE.md` na nivou repoa) konzervativnija.

---

### 3.2 Pitanje 2 — Da li je Varijanta B bolji upstream PR zato što direktno dodaje `OfferAsync(T, CancellationToken)` na `ISourceQueueWithComplete<T>`?

**Ne na `dev` grani; eventualno na major bumpu.** B je *bukvalno* bliže onome što issue traži, ali ima konkretne troškove koje A nema:

#### 3.2.1 Lomljenje implementatora

`grep -rn ": ISourceQueueWithComplete\| ISourceQueueWithComplete<"` po čitavom Akka.NET stablu pronalazi:
- `src/core/Akka.Streams/Implementation/Sources.cs:362` — `QueueSource.Materialized` (jedina implementacija u repou)
- `src/core/Akka.Streams/Queue.cs:52` — sama definicija interfejsa
- `src/core/Akka.Streams/Dsl/Source.cs:1056` — povratni tip iz `Source.Queue<T>` (consumer, ne implementer)
- `src/core/Akka.Streams.Tests/Dsl/QueueSourceSpec.cs:223` — `TestSourceStage<int, ISourceQueueWithComplete<int>>` (consumer)

Dakle, *u samom Akka.NET stablu* nema drugih implementatora — zato sve interno radi i u B.

Ali eksterni svet ga itekako implementira: bilo koji *test double* (Moq, NSubstitute, ručni stub), bilo koji *connector* koji omotava `ISourceQueueWithComplete<T>` (npr. unutar Akka.Streams.Kafka ili custom connector-a) **će se srušiti** dodavanjem nove apstraktne metode. Akka.NET je dovoljno raširen da se ovo ne sme zanemariti.

#### 3.2.2 Default Interface Method (DIM) ne pomaže

`Akka.Streams.csproj` cilja `$(NetStandardLibVersion);$(NetLibVersion)`, što rezolviše u `Directory.Build.props`:

```
NetStandardLibVersion = netstandard2.0
NetLibVersion         = net6.0
```

DIM za interfejse je dostupan tek od `netstandard2.1` / `net5+`. Sve dok `Akka.Streams` cilja `netstandard2.0`, **B ne može da ublaži lomljenje sa `default` implementacijom** — eksterni implementator nema izbora osim da implementira novi član ili da prestane da kompajlira.

#### 3.2.3 Politika `CLAUDE.md` u korenu repoa

`workspace/platform/akka.net/CLAUDE.md` (sekcija *API Design*):

> - Extend-only design - **don't modify existing public APIs**
> - Preserve wire format compatibility for serialization
> - Include unit tests with all changes

Ovo je *eksplicitna* projekt-konvencija. Varijanta B je u direktnom konfliktu sa drugim bullet-om. Varijanta A je u skladu — ona *proširuje* API (dodaje izvedeni interfejs i extension) ne diraj postojeći.

Aaron je labelirao issue `api-change`, što sugeriše da je svestan da je u igri promena javnog API-ja. Ali label `api-change` ne znači automatski "smemo da slomimo eksterne implementatore na minor verziji" — pogotovo na `1.5.x` liniji koja je trenutni `dev`. Sledi:

#### 3.2.4 Šta uraditi

- Otvoriti PR sa **Varijantom A**, jer ne troši deprecation-cikluse i ne tera korisnike da bumpaju major.
- U opisu PR-a navesti: *"Behavior is identical for `Source.Queue<T>` users; if you want a hard `OfferAsync` member on `ISourceQueueWithComplete<T>` itself, that is a major-bump change which I can produce as a follow-up against the next major branch."*
- Držati Varijantu B kao backup granu i predložiti je samo ako maintainer izričito kaže "ne, hoću čisti API, slomi šta treba".

Ovo daje maintainer-u izbor — bez gubljenja vremena na PR koji bi tražio major bump kad postoji clean alternativa.

---

### 3.3 Pitanje 3 — Da li je breaking change u Varijanti B prihvatljiv za Akka.NET public API?

**Tehnički je breaking; politika repoa kaže ne; pravac za potencijalno *da* postoji ali nije sada.**

Konkretno:

#### 3.3.1 Šta tačno se lomi

`CoreAPISpec.ApproveStreams.DotNet.verified.txt` (B), red 724–727:

```diff
 public interface ISourceQueueWithComplete<in T> : Akka.Streams.ISourceQueue<T>
 {
     void Complete();
     void Fail(System.Exception ex);
+    System.Threading.Tasks.Task<Akka.Streams.IQueueOfferResult> OfferAsync(T element, System.Threading.CancellationToken cancellationToken);
     new System.Threading.Tasks.Task WatchCompletionAsync();
 }
```

Identično u `*.Net.verified.txt`. Dakle ova izmena je *hard ABI break* za bilo kog spoljašnjeg implementora interfejsa — ApiApprovals-na sapun ovo automatski hvata kao "API surface changed".

`Microsoft.DotNet.PackageValidation` (i alternative tipa `Microsoft.CodeAnalysis.PublicApiAnalyzers`) bi ovo prijavili kao **major-bump-required** izmenu. Ako Akka.NET koristi SemVer striktno, B može da uđe samo u sledeći major.

#### 3.3.2 Šta verzija govori

`global.json` ne diktira Akka verziju, ali `RELEASE_NOTES.md` poslednjeg blokom kaže `1.5.47 August 12th, 2025`, sa sledećim "Placeholder for nightly build". Trenutni dev cilja **1.5.x liniju**, koja je formalno *minor*. Po SemVer-u: minor = no breaking changes na public API.

#### 3.3.3 Kompromis koji bi B učinio prihvatljivim

Ako maintainer-i baš žele B, ali na minor liniji, opcije:

a) Označiti `OfferAsync(T, CancellationToken)` na interfejsu kao virtual (DIM) — **ne radi** dok god je netstandard2.0 u skup ciljeva.
b) Bumpnuti minimalni netstandard target na `netstandard2.1` (Akka generalno to nije do sad uradio i to bi izbacilo netfx 4.7.2 korisnike).
c) Prihvatiti break uz jasan release-note, listing implementatora koji će morati da se ažuriraju, i možda pomoćni `[Obsolete]` interfejs pre samog cuta.

Ni jedna od ovih opcija nije besplatna. Praktično rešenje ostaje: A za sada, B kasnije.

#### 3.3.4 Zaključak za pitanje 3

Breaking change u B je *tehnički* prihvatljiv samo na major bumpu. Na trenutnoj `1.5.x` liniji nije, jer narušava i ABI i `CLAUDE.md` ekstend-only pravilo. Aaronov `api-change` label najverovatnije signalizuje *"znam da je u igri promena, voljan sam je primiti"*, ne *"slomi je gde god ti odgovara"* — odluka ostaje na maintainer-ima.

---

### 3.4 Pitanje 4 — Da li `cancellation` path u `QueueSource<T>.Logic` pravilno uklanja samo još-pending offer i ne može kasnije emitovati canceled element?

**Da. Provereno semantički + testovima u oba scenarija, sa jednom umešnom granicom oko `_terminating` koju treba dokumentovati.**

Cancellation path se sastoji iz tri faze:

#### 3.4.1 Faza 1 — pozivna strana (`Materialized.OfferAsync(elem, ct)`)

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
        var registration = cancellationToken.Register(() => _invokeLogic(new CancelOffer(offer, cancellationToken)));
        promise.Task.ContinueWith(_ => registration.Dispose(), TaskContinuationOptions.ExecuteSynchronously);
    }

    return promise.Task;
}
```

- Pre svega: **fast path** ako je token već canceled — vraća `Task.FromCanceled` *bez* da pošalje `Offer` na logiku. To je tačno semantika koju issue traži (offer se nikad ne enqueue-uje ako je token već gotov). Test `should_not_enqueue_offer_when_token_is_already_canceled` (`QueueSourceSpec.cs:471`) pokriva ovu granu.
- Ako token *nije* canceled ali *može* biti, registrujemo callback. Callback dispatch-uje **`CancelOffer` poruku** kroz isti `_invokeLogic` kanal kao i `Offer`, garantujući redoslednu obradu kroz interpreter dispatcher.
- `promise.Task.ContinueWith(... registration.Dispose() ..., ExecuteSynchronously)` osigurava da kada offer *ipak* uspe (Enqueued/Dropped/QueueClosed), CT registracija se otkači — sprečava memory-leak i sprečava da kasnije Cancel emituje "stale" `CancelOffer` (čak i da bi taj `CancelOffer` našao `_pendingOffer == null` i bio no-op, ne želimo nepotrebne poruke kroz callback queue).
  - Napomena: `NonBlockingTaskCompletionSource` postavlja `RunContinuationsAsynchronously`, što *poništava* `ExecuteSynchronously` hint i tera continuation na thread pool. To je *bezbednije* od strogo sinhronog disposal-a — nema rentrancy iz `TrySetResult` ka CT registracji. Disposal se desi malo kasnije, što je benigno.
- Trka između `_invokeLogic(offer)` i `cancellationToken.Register(...)`: ako CT zaista cancel-uje između te dve tačke, `Register` u .NET-u atomski detektuje već-canceled stanje i poziva callback **sinhronno pre povratka iz `Register`-a**. To znači da će `CancelOffer` biti dispatch-ovan u `_invokeLogic` queue *posle* `Offer`-a (jer offer je već postavljen), pa će redosled obrade biti `Offer` → `CancelOffer`. Korektno: ako Logic obradi `Offer` prvo i još uvek je u backpressure-u, postaviće `_pendingOffer = offer`; potom `CancelOffer` matche-uje target i otkazuje.

#### 3.4.2 Faza 2 — Logic obrada `CancelOffer`-a (`Sources.cs`)

```csharp
if (input is CancelOffer cancelOffer)
{
    if (!_terminating && _pendingOffer != null && ReferenceEquals(_pendingOffer, cancelOffer.Target))
    {
        _pendingOffer.CompletionSource.NonBlockingTrySetCanceled(cancelOffer.CancellationToken);
        _pendingOffer = null;
    }
}
```

Tri uslova svaki sa razlogom:

1. `!_terminating` — ako je stage u procesu zatvaranja (pre `CompleteStage`), offer mora da prođe kroz već postavljen drainage put (npr. `OnPull` izlije pending offer i postavi `Enqueued`). Test `should_ignore_cancellation_after_complete_was_requested_for_pending_offer` (`QueueSourceSpec.cs:478`) eksplicitno pokriva: `Complete()` se pošalje pre `Cancel()`, Logic obradi `Completion` prvi i postavi `_terminating = true`, potom `CancelOffer` dolazi i biva no-op.

2. `_pendingOffer != null` — ako offer nije pending (već je bio enqueued u buffer ili push-ovan downstream), nema šta da se otkaže. Test `should_ignore_cancellation_after_offer_was_enqueued` (`QueueSourceSpec.cs:457`) pokriva: bufferSize=1, prvi offer se odmah enqueue-uje (buffer ima mesta) → `_pendingOffer` ostaje `null` → naknadni `Cancel` ne menja stanje. Element je već *u buffer-u*, biće emitovan downstream-u. Posle `Cancel`-a `offer.Status` ostaje `RanToCompletion` sa rezultatom `Enqueued`.

3. `ReferenceEquals(_pendingOffer, cancelOffer.Target)` — kritičan invariant. Ako je *novi* offer u međuvremenu postao `_pendingOffer` (npr. korisnik je kanselirao stari, pa odmah pozvao novi offer), `CancelOffer` koji je nosio referencu na *stari* target *ne sme* da otkaže novi. `ReferenceEquals` to garantuje. Test `should_accept_next_backpressured_offer_after_pending_offer_is_canceled` (`QueueSourceSpec.cs:415`) pokriva ovaj precizan tok.

#### 3.4.3 Faza 3 — element ne curi nigde

Provera "da li canceled element može kasnije biti emitovan" se svodi na praćenje gde Element drži referencu:

- `Offer<TOut>.Element` — ovo je jedina referenca na korisnikov payload kad je offer *pending*.
- Kada `CancelOffer` matche-uje, kod radi `_pendingOffer = null;`. Posle toga ne postoji put kojim Logic može ponovo doći do tog `Offer<TOut>` instance-a:
  - `OnPull` čita iz `_pendingOffer` ili `_buffer`. `_pendingOffer` je sada `null`; element nije bio enqueue-ovan u `_buffer` jer je upravo bio "pending-pre-buffering" (zero-buffer slučaj) ili "pending-jer-je-buffer-bio-pun" (Backpressure kad `BufferElement` postavlja `_pendingOffer`).
  - U Backpressure putanji `Offer` handler-a koji bira `_pendingOffer = offer`, element nikad ne ulazi u `_buffer` u toj iteraciji. Test `should_cancel_pending_offer_when_backpressured_buffer_is_full` (`QueueSourceSpec.cs:362`) eksplicitno proverava: posle `Cancel`-a, `probe.Request(2)` vraća samo originalni element (`1`), ne canceled element (`2`). `probe.ExpectNoMsgAsync(_pause)` posle drugog request-a potvrđuje da `2` *nikad ne stiže downstream*.
  - U zero-buffer putanji (`_buffer == null`), `_pendingOffer = offer` postavlja se kad nema downstream demand-a. Push se dešava samo iz `OnPull` ili iz Offer handler-a kad je `IsAvailable(Out)`. Posle cancellation-a `_pendingOffer` je `null`, pa nijedan od ta dva puta ne emituje element. Test `should_cancel_pending_offer_when_backpressured_without_buffer` (`QueueSourceSpec.cs:391`) pokriva.

**Zaključak za pitanje 4:** cancellation path je korektno ograničen na *još-pending* offer i **ne emituje canceled element ni u jednom od sedam pokrivenih scenarija**. Tri uslovne grane oko `_terminating`/`_pendingOffer != null`/`ReferenceEquals` rade tačno onaj prelom koji semantika cancellation-a zahteva.

#### 3.4.4 Manja zapažanja koja ne menjaju zaključak ali su za implementere korisna

- `PostStop` → `StopCallback` ignoriše `CancelOffer` (proverava samo `is Offer<TOut>`). To je benigno: ako stage stane dok je `CancelOffer` u letu, `StopCallback` će obraditi pridruženi `Offer` (ako je još bio u queue-u) i dodeliti mu `StreamDetachedException` preko `NonBlockingTrySetException`. Disposal CT registracije će se desiti kroz `ContinueWith`. Cancel poslat *posle* stop-a samo silently ide u prazno — što je očekivano za zatvoren stage.
- `cancellationToken` se prenosi i u closure-u (radi dispatch-a `CancelOffer`-a) i kao field `CancelOffer.CancellationToken`. Ovo drugo je *neophodno* da bi `NonBlockingTrySetCanceled(cancelOffer.CancellationToken)` proizveo `OperationCanceledException` sa odgovarajućim tokenom. Test `AssertCanceledWithToken` upravo to verifikuje (`exception.CancellationToken.Should().Be(cancellationToken)`).

---

### 3.5 Pitanje 5 — Da li su testovi u `QueueSourceSpec` dovoljni za backpressure, zero-buffer, already-canceled token, cancellation race, completion, and enqueue no-op semantics?

**Za behavior surface-a `Materialized.OfferAsync(elem, ct)` — da, sa jednim malim gepom u Varijanti A. Za defensive grane Varijante A ekstenzione metode — ne, fali tri test slučaja.**

#### 3.5.1 Mapiranje 7 novih testova na semantičke uslove

| # | Test | Pokriva |
|---|---|---|
| 1 | `should_cancel_pending_offer_when_backpressured_buffer_is_full` | bufferSize > 0, OverflowStrategy.Backpressure, drugi offer pending, cancel; verifikuje `IsCanceled`, `OperationCanceledException.CancellationToken`, da downstream ne dobija canceled element |
| 2 | `should_cancel_pending_offer_when_backpressured_without_buffer` | bufferSize == 0, isto + downstream request posle cancel-a ne dobija ništa |
| 3 | `should_accept_next_backpressured_offer_after_pending_offer_is_canceled` | state-machine recovery: posle cancel-a, novi offer prolazi normalno; `ReferenceEquals` invariant |
| 4 | `should_not_enqueue_offer_when_token_is_already_canceled` | fast-path `IsCancellationRequested` u `OfferAsync`; element ne ulazi u stream |
| 5 | `should_ignore_cancellation_after_offer_was_enqueued` | post-enqueue cancel je no-op; `Status == RanToCompletion`, `Result == Enqueued` |
| 6 | `should_ignore_cancellation_after_complete_was_requested_for_pending_offer` | `_terminating` ima prioritet nad cancel-om; pending offer drain-uje normalno |
| 7 | `should_cancel_offer_when_token_fires_before_stage_processes_offer` | trka: drugi offer queued, cancel pre nego stage ima vremena da ga obradi (map blocking); verifikuje da stage normalno obrađuje prvi offer i zatim cancel-uje drugi |

Pokrivenost grana iz pitanja:
- **Backpressure (with/without buffer)**: testovi 1, 2 ✓
- **Zero-buffer**: testovi 2, 4, 6, 7 ✓
- **Already-canceled token**: test 4 ✓
- **Cancellation race**: testovi 3 (state recovery), 7 (queue-order race) ✓
- **Completion takes precedence**: test 6 ✓
- **Enqueue no-op (cancel after enqueue)**: test 5 ✓

Sve fundamentalne grane su pokrivene.

#### 3.5.2 Šta nedostaje

**Za obe varijante:**
- ❗ `CancellationToken.None` (default) preko *novog* OfferAsync overload-a nije eksplicitno testiran. Sve postojeće `source.OfferAsync(1)` pozivi koriste *stari* API. Nigde u testovima ne nalazimo `source.OfferAsync(1, CancellationToken.None)` ili `source.OfferAsync(1, default)`. Implementacija u `Sources.cs` to obrađuje (`if (cancellationToken.CanBeCanceled)` granu — kad je `None`, `CanBeCanceled` je `false`, pa se preskače registracija). Trivijalan smoke test bi tu granu pokrio:

  ```csharp
  // Ne mora da bude nov [Fact], dovoljno je dodati u jedan postojeći test
  AssertSuccess(source.OfferAsync(1, CancellationToken.None));
  ```

- ⚠ Test 7 (`should_cancel_offer_when_token_fires_before_stage_processes_offer`) je *intencionalno* fragilan oko thread sleep-eva (`mapStarted.Wait(3s)`, `releaseMap.Wait(3s)`). To je legitimna sinhronizaciona šema (`ManualResetEventSlim` sa timeout-om), ne `Thread.Sleep` heuristika, ali bi pod CI opterećenjem mogao da postane flaky ako se 3s prekorači. Vredno je u PR komentaru pomenuti ovu sliku za reviewer-a.

**Specifično za Varijantu A — extension metoda u `Queue.cs`:**
Tri grane defensive koda u `SourceQueueWithCompleteExtensions.OfferAsync` nemaju dedicated test:

a) `if (sourceQueue is null) throw new ArgumentNullException(...)` — nepokriveno.

b) `if (!cancellationToken.CanBeCanceled) return sourceQueue.OfferAsync(element);` — nepokriveno (jedini "ne-cancel" test koristi `CancellationToken.None`, ali ide kroz `Materialized.OfferAsync(elem, ct)` ne kroz extension).

c) `throw new NotSupportedException(...)` kad implementator nije `ICancellableSourceQueueWithComplete<T>` — nepokriveno (svi testovi koriste `Source.Queue<T>` čija je `Materialized` *jeste* `ICancellable...`).

Predlog testa za (c) (samo za Varijantu A):

```csharp
[Fact]
public void OfferAsync_extension_throws_NotSupportedException_for_non_cancellable_implementer()
{
    var legacyQueue = new LegacySourceQueueStub<int>(); // implementira samo ISourceQueueWithComplete<int>
    var act = () => legacyQueue.OfferAsync(1, new CancellationTokenSource().Token);
    act.Should().ThrowAsync<NotSupportedException>();
}
```

Predlog za (a):

```csharp
[Fact]
public void OfferAsync_extension_throws_ArgumentNullException_for_null_queue()
{
    ISourceQueueWithComplete<int> queue = null!;
    var act = () => queue.OfferAsync(1, default);
    act.Should().ThrowAsync<ArgumentNullException>();
}
```

#### 3.5.3 Predlog *jednog* dodatnog testa zajedničkog za obe varijante

Multi-token concurrency: dva istovremena `OfferAsync(elem, ct)` poziva sa različitim tokenima u Backpressure modu. Postojeći Akka semantika (`IllegalStateException("You have to wait for previous offer to be resolved...")`) treba da i dalje važi. Test bi otkrio bilo kakvu regresiju u tom sloju kad se kombinuje sa cancellation-om.

```csharp
[Fact]
public async Task QueueSource_should_reject_concurrent_offer_with_IllegalStateException_even_when_first_is_canceled()
{
    await this.AssertAllStagesStoppedAsync(async () => {
        var tuple = Source.Queue<int>(0, OverflowStrategy.Backpressure)
            .ToMaterialized(this.SinkProbe<int>(), Keep.Both)
            .Run(_materializer);
        var source = tuple.Item1; var probe = tuple.Item2;

        using var cts1 = new CancellationTokenSource();
        var first = source.OfferAsync(1, cts1.Token);
        await ExpectNoMsgAsync(_pause);

        var second = source.OfferAsync(2);
        await Assert.ThrowsAsync<IllegalStateException>(async () => await second);

        cts1.Cancel();
        await AssertCanceledWithToken(first, cts1.Token);

        source.Complete();
        await probe.ExpectCompleteAsync();
    }, _materializer);
}
```

Ovo bi učvrstilo postojeću *resolution-ordering* invariant koju sigurno koriste real korisnici (Slack/webhook adapters iz issue opisa).

---

## 4. Tragovi AI-slop / namestanja rezultata

Eksplicitno traženo: provera da li je rezultat fabrikovan, nameštan ili "izmišljen". Pregledao sam:

- ✅ **Sve referencirane SHA cifre stvarno postoje** u repou (`git rev-parse` potvrđuje).
- ✅ **Tests stvarno prolaze** — pokrenuto nezavisno na obe grane (`net10.0`), `33/33` + `18/18`, `Skipped: 0`, `Failed: 0`.
- ✅ **Nema `Skip(...)`, `Trait("Skip"...)`, `[Fact(Skip = ...)]`, `#if FALSE` u dodatim testovima** (`grep`-om kroz `git diff`).
- ✅ **Nema `Thread.Sleep(...)` heuristika u dodatim testovima**. Postoje dva `ManualResetEventSlim.Wait(3s)` u test #7, što je legitiman sinhronizacioni primitive, ne maskiranje race-a.
- ✅ **`AssertCanceledWithToken` helper poziva `Assert.ThrowsAnyAsync<OperationCanceledException>` i verifikuje `exception.CancellationToken.Should().Be(cancellationToken)`** — to je striktno i ne svodi se na "task.IsCanceled.Should().BeTrue()" koje bi olabavljen test mogao da koristi.
- ✅ **Tests pokrivaju i no-op semantiku** (test #5 verifikuje da posle cancel-a nakon enqueue-a, `Status == RanToCompletion`, `Result == Enqueued`) — što je pozitivna provera da implementacija ne briše stanja koja ne treba da briše.
- ✅ **Implementacija u `Sources.cs` je pravi state-handler kod**, ne stub: `_pendingOffer = null` se eksplicitno postavlja, `NonBlockingTrySetCanceled(cancelOffer.CancellationToken)` se *zaista* poziva sa pravim tokenom (ne `default`).
- ✅ **`TaskEx.NonBlockingTrySetCanceled` ekstenzija je tanak wrapper** oko `TrySetCanceled(CancellationToken)` koji ide u liniji sa postojećim `NonBlockingTrySetResult`, `NonBlockingTrySetException` šablonima — ne nešto novo i sumnjivo.
- ✅ **API approval fajlovi su konsistentni** između `DotNet` i `Net` varijanti (oba dodaju iste linije; jedini ekstra "diff" je `\No newline at end of file` koji je benigan code-cleanup).

Stvar koju **JESAM** detektovao (ali nije slop, već vredna napomene za review-era):
- Varijanta B u poslednjoj liniji `*.verified.txt` fajlova menja `\ No newline at end of file` → sa novim line-om. To nije logička izmena ApiApprovals, samo tekstualni cleanup. Maintainer može želeti da se to commit-uje kao zaseban `[chore]` patch ili da se ostavi tako kako jeste.
- ContinueWith za `registration.Dispose` koristi `TaskContinuationOptions.ExecuteSynchronously`, ali baseni TaskCompletionSource je kreiran sa `RunContinuationsAsynchronously`. To efektivno prepravlja `ExecuteSynchronously` u async dispatch. Nije bug, ali implementer može želeti da odluči da li je to namera ili je `ExecuteSynchronously` bio mišljen kao `RunContinuationsAsynchronously`-ne-postoji slučaj. Ja smatram da je sigurno (sprečava reentrancy iz set-result u CT registraciju).

**Zaključak za sekciju 4:** Nisam pronašao nijedan trag namestanja rezultata, fabrikovanja brojeva, fake assertion-a, niti zataškavanja test failure-a. Patch je *iskreno napravljen*.

---

## 5. Konkretne preporuke implementatorima

Po prioritetu:

### P0 — Blokirajuće za upstream PR

- **Otvoriti PR sa Varijantom A**, sa porukom commit-a koja eksplicitno citira `CLAUDE.md` *Extend-only design* pravilo, i objašnjava zašto to opt-in pristup poštuje, dok se semantička metoda `OfferAsync(T, CancellationToken)` i dalje rezolviše prirodno preko ekstenzije za korisnike `Source.Queue<T>`.
- U PR opisu navesti: "Varijanta B (direct interface mutation) postoji u grani `fix/8081-source-queue-direct-api`; spremna je za sledeći major. Ako maintainer-i preferiraju B, mogu rebase-ovati na trenutni dev."

### P1 — Pre merge-a u upstream

- **Dodati 2-3 testa za defensive grane Varijante A ekstenzione metode** (sekcija 3.5.2): `null sourceQueue`, `!CanBeCanceled`, `NotSupportedException` za ne-Cancellable implementora. Ti testovi su jeftini i pokrivaju 100% novih javnih grana.
- **Dodati jedan test koji koristi `OfferAsync(elem, CancellationToken.None)`** — pokriva fast-path `!CanBeCanceled` u `Materialized.OfferAsync`. Trivijalno: zameniti jedan postojeći `source.OfferAsync(1)` sa `source.OfferAsync(1, default)` i potvrditi `Enqueued`.

### P2 — Stilski / nice-to-have

- **Test #7 (`should_cancel_offer_when_token_fires_before_stage_processes_offer`)** je vredan ali zavisi od `releaseMap.Wait(3s)` — pod jako opterećenim CI runner-ima ovo može da padne flaky. U PR opisu napomenuti da je `Trait("Category", "TimingSensitive")` opciono možda dobro dodati ako Akka.NET takvu kategoriju koristi.
- **`OfferAsync(TOut element)` u `Materialized` postaje thin wrapper za `OfferAsync(element, CancellationToken.None)`**. Nema regresije, ali je dobra mikro-tačka u commit message-u: "non-cancellable callers now go through the same hot-path as cancellable ones, so behavior is bit-for-bit identical to upstream when CT is None."
- **`RELEASE_NOTES.md` u Varijanti A pominje *both* `ICancellableSourceQueueWithComplete<T>` i extension metodu**. To je dobro za community discoverability. Varijanta B-jev release note govori samo o metodi — ako neki put se ipak ide na B, vredi proširiti note-u sa "this is a breaking change for external implementors of `ISourceQueueWithComplete<T>`; please update them to implement the new member" i listu poznatih implementora ako ih maintainer može efikasno mapirati.

### P3 — Out of scope, ali za maintainere

- Razmotriti ima li smisla `Source.CancellableQueue<T>(...)` factory koji vraća `Source<T, ICancellableSourceQueueWithComplete<T>>` — to bi za nove korisnike eliminisalo extension method posrednika. Ne predlog za ovaj PR; pitanje za roadmap.
- Razmotriti kako `Sink.Queue` (suprotan smer) tretira backpressure i da li korisnici imaju sličnu potrebu za cancellation tamo. Posebno za sledeći issue.

---

## 6. Hash sažetak za reproducibilnost

```
upstream/dev:                                 e7366a0389157d78993f0222a83248ed076fd45b
fix/8081-source-queue-offer-cancellation (A): 86b275af85b036dcc34cb54a02becce991cb9388
fix/8081-source-queue-direct-api         (B): d430a6035a7f9234057b6143bba307edf061dc3f

QueueSourceSpec (net10.0):  A:33/33   B:33/33
Akka.API.Tests (net10.0):   A:18/18   B:18/18
```

Sve diff komande iz originalnog brifa sam pokrenuo i upoređivao sa direktnim čitanjem fajlova; ovaj pregled je grounded u stvarnom radnom drvetu, ne u sećanju ili pretpostavkama.
