//-----------------------------------------------------------------------
// <copyright file="SubFlowShapePrototypeSpec.cs" company="Akka.NET Project">
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
    public class SubFlowShapePrototypeSpec : AkkaSpec
    {
        private ActorMaterializer Materializer { get; }

        public SubFlowShapePrototypeSpec(ITestOutputHelper helper)
            : base(helper)
        {
            var settings = ActorMaterializerSettings.Create(Sys).WithInputBuffer(2, 2);
            Materializer = ActorMaterializer.Create(Sys, settings);
        }

        [Fact]
        public async Task Source_subflow_must_preserve_source_shape_through_representative_subflow_operations()
        {
            await this.AssertAllStagesStoppedAsync(async () =>
            {
                Source<long, Task<Done>> source = Source.From(Enumerable.Range(1, 100))
                    .GroupBy(10, value => value % 10)
                    .Select(value => (long)value)
                    .Sum((left, right) => left + right)
                    .MergeSubstreams()
                    .WireTapMaterialized(Sink.ForEach<long>(_ => { }), Keep.Right);

                var (tapCompletion, elementsTask) = source
                    .ToMaterialized(Sink.Seq<long>(), Keep.Both)
                    .Run(Materializer);

                (await tapCompletion).Should().Be(Done.Instance);
                AssertMergedElements(await elementsTask);
            }, Materializer);
        }

        [Fact]
        public async Task Flow_subflow_must_preserve_flow_shape_through_representative_subflow_operations()
        {
            await this.AssertAllStagesStoppedAsync(async () =>
            {
                Flow<int, long, Task<Done>> flow = Flow.Create<int>()
                    .GroupBy(10, value => value % 10)
                    .Select(value => (long)value)
                    .Sum((left, right) => left + right)
                    .MergeSubstreams()
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
        public async Task Substream_factories_must_return_typed_source_and_flow_subflows()
        {
            await this.AssertAllStagesStoppedAsync(() =>
            {
                SourceSubFlow<int, NotUsed> groupedSource = Source.From(Enumerable.Range(1, 10))
                    .GroupBy(2, value => value % 2);
                SourceSubFlow<int, NotUsed> splitWhenSource = Source.From(Enumerable.Range(1, 10))
                    .SplitWhen(value => value % 2 == 0);
                SourceSubFlow<int, NotUsed> splitAfterSource = Source.From(Enumerable.Range(1, 10))
                    .SplitAfter(value => value % 2 == 0);

                FlowSubFlow<int, int, NotUsed> groupedFlow = Flow.Create<int>()
                    .GroupBy(2, value => value % 2);
                FlowSubFlow<int, int, NotUsed> splitWhenFlow = Flow.Create<int>()
                    .SplitWhen(value => value % 2 == 0);
                FlowSubFlow<int, int, NotUsed> splitAfterFlow = Flow.Create<int>()
                    .SplitAfter(value => value % 2 == 0);

                groupedSource.Should().NotBeNull();
                splitWhenSource.Should().NotBeNull();
                splitAfterSource.Should().NotBeNull();
                groupedFlow.Should().NotBeNull();
                splitWhenFlow.Should().NotBeNull();
                splitAfterFlow.Should().NotBeNull();

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
