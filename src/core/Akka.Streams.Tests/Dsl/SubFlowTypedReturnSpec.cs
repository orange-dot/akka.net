//-----------------------------------------------------------------------
// <copyright file="SubFlowTypedReturnSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
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
    /// Verifies the typed SourceSubFlow/FlowSubFlow subtypes (akka.net issue #5381): the concrete
    /// Source/Flow shape is preserved through intermediate substream operators, so
    /// MergeSubstreams returns the concrete type with no cast. The explicitly typed locals are the
    /// load-bearing assertions; if this spec compiles, the type was restored.
    /// </summary>
    public class SubFlowTypedReturnSpec : AkkaSpec
    {
        private static readonly long[] ExpectedGroupSums = { 460, 470, 480, 490, 500, 510, 520, 530, 540, 550 };

        private ActorMaterializer Materializer { get; }

        public SubFlowTypedReturnSpec(ITestOutputHelper helper) : base(helper)
        {
            var settings = ActorMaterializerSettings.Create(Sys).WithInputBuffer(2, 2);
            Materializer = ActorMaterializer.Create(Sys, settings);
        }

        [Fact]
        public async Task MergeSubstreams_must_return_concrete_Source_through_operators_without_cast()
        {
            await this.AssertAllStagesStoppedAsync(async () =>
            {
                // GroupBy -> Where -> Sum -> MergeSubstreams stays SourceSubFlow the whole way, so
                // MergeSubstreams returns Source<long, NotUsed> with no cast (the #5381 fix).
                Source<long, NotUsed> typed = Source.From(Enumerable.Range(1, 100).Select(i => (long)i))
                    .GroupBy(10, l => l % 10)
                    .Where(_ => true)
                    .Sum((l, r) => l + r)
                    .MergeSubstreams();

                var task = typed.RunWith(Sink.Seq<long>(), Materializer);
                await task.WaitAsync(3.Seconds());
                task.Result.OrderBy(x => x).Should().Equal(ExpectedGroupSums);
            }, Materializer);
        }

        [Fact]
        public async Task MergeSubstreams_must_chain_into_WireTapMaterialized_without_cast()
        {
            await this.AssertAllStagesStoppedAsync(async () =>
            {
                // The exact issue #5381 scenario: WireTapMaterialized binds directly on the merged Source.
                Source<long, Task<Done>> typed = Source.From(Enumerable.Range(1, 100).Select(i => (long)i))
                    .GroupBy(10, l => l % 10)
                    .Sum((l, r) => l + r)
                    .MergeSubstreams()
                    .WireTapMaterialized(Sink.ForEach<long>(_ => { }), Keep.Right);

                var task = typed.RunWith(Sink.Seq<long>(), Materializer);
                await task.WaitAsync(3.Seconds());
                task.Result.OrderBy(x => x).Should().Equal(ExpectedGroupSums);
            }, Materializer);
        }

        [Fact]
        public async Task MergeSubstreams_must_preserve_an_element_type_change_to_Source()
        {
            await this.AssertAllStagesStoppedAsync(async () =>
            {
                // Grouped changes the element type to IEnumerable<long>; the result is still a Source.
                Source<IEnumerable<long>, NotUsed> typed = Source.From(Enumerable.Range(1, 100).Select(i => (long)i))
                    .GroupBy(2, l => l % 2)
                    .Grouped(100)
                    .MergeSubstreams();

                var task = typed.RunWith(Sink.Seq<IEnumerable<long>>(), Materializer);
                await task.WaitAsync(3.Seconds());
                task.Result.Should().HaveCount(2);
                task.Result.SelectMany(x => x).Sum().Should().Be(5050);
            }, Materializer);
        }

        [Fact]
        public async Task MergeSubstreams_must_return_concrete_Flow_through_operators_without_cast()
        {
            await this.AssertAllStagesStoppedAsync(async () =>
            {
                Flow<int, long, NotUsed> typed = Flow.Create<int>()
                    .Select(i => (long)i)
                    .GroupBy(10, l => l % 10)
                    .Sum((l, r) => l + r)
                    .MergeSubstreams();

                var task = Source.From(Enumerable.Range(1, 100))
                    .Via(typed)
                    .RunWith(Sink.Seq<long>(), Materializer);
                await task.WaitAsync(3.Seconds());
                task.Result.OrderBy(x => x).Should().Equal(ExpectedGroupSums);
            }, Materializer);
        }
    }
}
