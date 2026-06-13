//-----------------------------------------------------------------------
// <copyright file="FlowSubFlow.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

namespace Akka.Streams.Dsl
{
    /// <summary>
    /// A <see cref="SubFlow{TOut,TMat,TClosed}"/> rooted in a <see cref="Flow{TIn,TOut,TMat}"/>.
    ///
    /// <para>
    /// This subtype recovers the "open witness" that the base <see cref="SubFlow{TOut,TMat,TClosed}"/>
    /// erases to <see cref="IFlow{TOut,TMat}"/>: closing the substream scope by merging returns the
    /// concrete <see cref="Flow{TIn,TOut,TMat}"/> again, so the fluent builder can continue without a
    /// cast (akka.net issue #5381). The merge methods are covariant overrides of the base ones —
    /// legal because <see cref="Flow{TIn,TOut,TMat}"/> implements <see cref="IFlow{TOut,TMat}"/>.
    /// </para>
    /// </summary>
    /// <typeparam name="TIn">The input element type of the rooting <see cref="Flow{TIn,TOut,TMat}"/>.</typeparam>
    /// <typeparam name="TOut">The element type carried by the substream.</typeparam>
    /// <typeparam name="TMat">The materialized value type carried by the super-flow.</typeparam>
    public abstract class FlowSubFlow<TIn, TOut, TMat> : SubFlow<TOut, TMat, Sink<TIn, TMat>>
    {
        /// <inheritdoc cref="SubFlow{TOut,TMat,TClosed}.MergeSubstreamsWithParallelism"/>
        public abstract override Flow<TIn, TOut, TMat> MergeSubstreamsWithParallelism(int parallelism);

        /// <inheritdoc cref="SubFlow{TOut,TMat,TClosed}.MergeSubstreams"/>
        public override Flow<TIn, TOut, TMat> MergeSubstreams() => MergeSubstreamsWithParallelism(int.MaxValue);

        /// <inheritdoc cref="SubFlow{TOut,TMat,TClosed}.ConcatSubstream"/>
        public override Flow<TIn, TOut, TMat> ConcatSubstream() => MergeSubstreamsWithParallelism(1);
    }
}
