//-----------------------------------------------------------------------
// <copyright file="SourceSubFlowImpl.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System;
using Akka.Streams.Dsl;

namespace Akka.Streams.Implementation
{
    /// <summary>
    /// A type-preserving decorator over a <see cref="Source{TOut,TMat}"/>-rooted
    /// <see cref="SubFlow{TOut,TMat,TClosed}"/>. Forwards every member to the wrapped substream, but
    /// (a) re-wraps the result of <see cref="Via{T2,TMat2}"/> so the concrete subtype survives operator
    /// chaining, and (b) narrows the merge methods to the concrete <see cref="Source{TOut,TMat}"/>.
    ///
    /// <para>
    /// The merge downcast is sound: the wrapped substream's merge already produces a
    /// <see cref="Source{TOut,TMat}"/> at runtime (the merge-back applies <c>_self.Via(...)</c> on the
    /// original source); only the static type was widened to <see cref="IFlow{TOut,TMat}"/>.
    /// </para>
    /// </summary>
    /// <typeparam name="TOut">The element type carried by the substream.</typeparam>
    /// <typeparam name="TMat">The materialized value type carried by the super-flow.</typeparam>
    public class SourceSubFlowImpl<TOut, TMat> : SourceSubFlow<TOut, TMat>
    {
        private readonly SubFlow<TOut, TMat, IRunnableGraph<TMat>> _inner;

        /// <summary>
        /// Creates a decorator over the given <see cref="Source{TOut,TMat}"/>-rooted substream.
        /// </summary>
        /// <param name="inner">The substream to wrap.</param>
        public SourceSubFlowImpl(SubFlow<TOut, TMat, IRunnableGraph<TMat>> inner) => _inner = inner;

        /// <inheritdoc/>
        public override IFlow<T2, TMat> Via<T2, TMat2>(IGraph<FlowShape<TOut, T2>, TMat2> flow) =>
            new SourceSubFlowImpl<T2, TMat>((SubFlow<T2, TMat, IRunnableGraph<TMat>>)_inner.Via(flow));

        /// <inheritdoc/>
        public override IFlow<T2, TMat3> ViaMaterialized<T2, TMat2, TMat3>(IGraph<FlowShape<TOut, T2>, TMat2> flow, Func<TMat, TMat2, TMat3> combine) =>
            _inner.ViaMaterialized(flow, combine);

        /// <inheritdoc/>
        public override IFlow<TOut, TMat2> MapMaterializedValue<TMat2>(Func<TMat, TMat2> mapFunc) =>
            _inner.MapMaterializedValue(mapFunc);

        /// <inheritdoc/>
        public override TMat2 RunWith<TMat2>(IGraph<SinkShape<TOut>, TMat2> sink, IMaterializer materializer) =>
            _inner.RunWith(sink, materializer);

        /// <inheritdoc/>
        public override IRunnableGraph<TMat> To<TMat2>(IGraph<SinkShape<TOut>, TMat2> sink) =>
            _inner.To(sink);

        /// <inheritdoc/>
        public override Source<TOut, TMat> MergeSubstreamsWithParallelism(int parallelism) =>
            (Source<TOut, TMat>)_inner.MergeSubstreamsWithParallelism(parallelism);
    }
}
