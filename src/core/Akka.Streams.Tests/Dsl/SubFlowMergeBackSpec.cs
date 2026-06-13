//-----------------------------------------------------------------------
// <copyright file="SubFlowMergeBackSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Akka.TestKit;
using Akka.TestKit.Extensions;
using FluentAssertions;
using FluentAssertions.Extensions;
using Xunit;

namespace Akka.Streams.Tests.Dsl
{
    /// <summary>
    /// Verifies the type-preserving <c>MergeBack</c> extensions (akka.net issue #5381):
    /// closing a substream scope by merging must yield the concrete <see cref="Source{TOut,TMat}"/>
    /// / <see cref="Flow{TIn,TOut,TMat}"/> again, with no cast, even when operators sit between the
    /// substream entry point and the merge.
    ///
    /// <para>
    /// The explicitly typed locals are the load-bearing assertions: if this spec compiles, the static
    /// type was restored. The runtime assertions guard that the sound downcast also behaves.
    /// </para>
    /// </summary>
    public class SubFlowMergeBackSpec : AkkaSpec
    {
        // Sum-per-group of 1..100 keyed by (n % 10): group 0 = {10,20,..,100} = 550; group k (1..9) = 450 + 10k.
        private static readonly long[] ExpectedGroupSums = { 460, 470, 480, 490, 500, 510, 520, 530, 540, 550 };

        private ActorMaterializer Materializer { get; }

        public SubFlowMergeBackSpec(ITestOutputHelper helper) : base(helper)
        {
            var settings = ActorMaterializerSettings.Create(Sys).WithInputBuffer(2, 2);
            Materializer = ActorMaterializer.Create(Sys, settings);
        }

        [Fact]
        public async Task MergeBack_must_return_a_concrete_Source_without_a_cast()
        {
            await this.AssertAllStagesStoppedAsync(async () =>
            {
                // Compile-time proof: the chain GroupBy -> Sum -> MergeBack -> WireTapMaterialized binds
                // to Source<long, Task<Done>> with no cast. Before the fix this required
                // ((Source<long, Task<Done>>)source) because MergeSubstreams returned IFlow.
                Source<long, Task<Done>> typed = Source.From(Enumerable.Range(1, 100).Select(i => (long)i))
                    .GroupBy(10, l => l % 10)
                    .Sum((l, r) => l + r)
                    .MergeBack()
                    .WireTapMaterialized(Sink.ForEach<long>(_ => { }), Keep.Right);

                var task = typed.RunWith(Sink.Seq<long>(), Materializer);
                await task.WaitAsync(3.Seconds());
                task.Result.OrderBy(x => x).Should().Equal(ExpectedGroupSums);
            }, Materializer);
        }

        [Fact]
        public async Task MergeBack_must_return_a_concrete_Flow_without_a_cast()
        {
            await this.AssertAllStagesStoppedAsync(async () =>
            {
                // Compile-time proof: a Flow-rooted substream merges back to Flow<int, long, NotUsed>.
                Flow<int, long, NotUsed> typed = Flow.Create<int>()
                    .Select(i => (long)i)
                    .GroupBy(10, l => l % 10)
                    .Sum((l, r) => l + r)
                    .MergeBack();

                var task = Source.From(Enumerable.Range(1, 100))
                    .Via(typed)
                    .RunWith(Sink.Seq<long>(), Materializer);
                await task.WaitAsync(3.Seconds());
                task.Result.OrderBy(x => x).Should().Equal(ExpectedGroupSums);
            }, Materializer);
        }

        [Fact]
        public async Task MergeBack_must_work_for_a_SplitWhen_rooted_substream()
        {
            await this.AssertAllStagesStoppedAsync(async () =>
            {
                // Covers a substream entry point other than GroupBy; TClosed still discriminates the root.
                Source<int, NotUsed> typed = Source.From(Enumerable.Range(1, 10))
                    .SplitWhen(i => i % 3 == 0)
                    .MergeBack();

                var task = typed.RunWith(Sink.Seq<int>(), Materializer);
                await task.WaitAsync(3.Seconds());
                task.Result.OrderBy(x => x).Should().Equal(Enumerable.Range(1, 10));
            }, Materializer);
        }

        [Fact]
        public async Task MergeBack_must_accept_an_explicit_parallelism_and_preserve_the_type()
        {
            await this.AssertAllStagesStoppedAsync(async () =>
            {
                // The parallelism overload binds and is equally type-preserving. Keep the group count
                // within the merge breadth: Sum emits only at substream completion, so merging fewer
                // groups than the breadth avoids the documented GroupBy back-pressure deadlock.
                Source<long, NotUsed> typed = Source.From(Enumerable.Range(1, 100).Select(i => (long)i))
                    .GroupBy(2, l => l % 2)
                    .Sum((l, r) => l + r)
                    .MergeBack(parallelism: 4);

                // Odd sum (1,3,..,99) = 2500; even sum (2,4,..,100) = 2550.
                var task = typed.RunWith(Sink.Seq<long>(), Materializer);
                await task.WaitAsync(3.Seconds());
                task.Result.OrderBy(x => x).Should().Equal(2500L, 2550L);
            }, Materializer);
        }
    }
}
