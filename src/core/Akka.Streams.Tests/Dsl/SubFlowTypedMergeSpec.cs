//-----------------------------------------------------------------------
// <copyright file="SubFlowTypedMergeSpec.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Streams.Dsl;
using Akka.Streams.TestKit;
using Akka.TestKit;
using FluentAssertions;
using Xunit;

namespace Akka.Streams.Tests.Dsl
{
    public class SubFlowTypedMergeSpec : AkkaSpec
    {
        private ActorMaterializer Materializer { get; }

        public SubFlowTypedMergeSpec(ITestOutputHelper helper)
            : base(helper)
        {
            var settings = ActorMaterializerSettings.Create(Sys).WithInputBuffer(2, 2);
            Materializer = ActorMaterializer.Create(Sys, settings);
        }

        [Fact]
        public async Task MergeSubstreamsAsSource_must_preserve_the_source_builder_after_subflow_operations()
        {
            await this.AssertAllStagesStoppedAsync(async () =>
            {
                Source<long, Task<Done>> source = Source.From(Enumerable.Range(1, 100))
                    .Select(i => (long)i)
                    .GroupBy(10, value => value % 10)
                    .Sum((left, right) => left + right)
                    .MergeSubstreamsAsSource()
                    .WireTapMaterialized(Sink.ForEach<long>(_ => { }), Keep.Right);

                var (tapCompletion, elementsTask) = source
                    .ToMaterialized(Sink.Seq<long>(), Keep.Both)
                    .Run(Materializer);

                (await tapCompletion).Should().Be(Done.Instance);
                AssertMergedElements(await elementsTask);
            }, Materializer);
        }

        [Fact]
        public async Task MergeSubstreamsAsFlow_must_preserve_the_flow_builder_after_subflow_operations()
        {
            await this.AssertAllStagesStoppedAsync(async () =>
            {
                Flow<int, long, Task<Done>> flow = Flow.Create<int>()
                    .Select(i => (long)i)
                    .GroupBy(10, value => value % 10)
                    .Sum((left, right) => left + right)
                    .MergeSubstreamsAsFlow()
                    .WireTapMaterialized(Sink.ForEach<long>(_ => { }), Keep.Right);

                var (tapCompletion, elementsTask) = Source.From(Enumerable.Range(1, 100))
                    .ViaMaterialized(flow, Keep.Right)
                    .ToMaterialized(Sink.Seq<long>(), Keep.Both)
                    .Run(Materializer);

                (await tapCompletion).Should().Be(Done.Instance);
                AssertMergedElements(await elementsTask);
            }, Materializer);
        }

        [Fact]
        public async Task Typed_subflow_merge_helpers_must_cover_parallelism_and_concat_variants()
        {
            await this.AssertAllStagesStoppedAsync(() =>
            {
                Source<int, NotUsed> sourceWithParallelism = Source.From(Enumerable.Range(1, 10))
                    .GroupBy(2, value => value % 2)
                    .MergeSubstreamsWithParallelismAsSource(1);

                Flow<int, int, NotUsed> flowWithParallelism = Flow.Create<int>()
                    .GroupBy(2, value => value % 2)
                    .MergeSubstreamsWithParallelismAsFlow(1);

                Source<int, NotUsed> concatSource = Source.From(Enumerable.Range(1, 10))
                    .SplitAfter(_ => true)
                    .ConcatSubstreamAsSource();

                Flow<int, int, NotUsed> concatFlow = Flow.Create<int>()
                    .SplitAfter(_ => true)
                    .ConcatSubstreamAsFlow();

                sourceWithParallelism.Should().NotBeNull();
                flowWithParallelism.Should().NotBeNull();
                concatSource.Should().NotBeNull();
                concatFlow.Should().NotBeNull();

                return Task.CompletedTask;
            }, Materializer);
        }

        private static void AssertMergedElements(IImmutableList<long> elements)
        {
            elements.Should().HaveCount(10);
            elements.Sum().Should().Be(5050L);
        }
    }
}
