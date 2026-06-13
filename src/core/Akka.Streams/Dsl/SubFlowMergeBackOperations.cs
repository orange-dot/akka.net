//-----------------------------------------------------------------------
// <copyright file="SubFlowMergeBackOperations.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable

namespace Akka.Streams.Dsl
{
    /// <summary>
    /// Type-preserving variants of <see cref="SubFlow{TOut,TMat,TClosed}.MergeSubstreams"/> that
    /// recover the concrete DSL surface when a substream scope is closed by merging.
    ///
    /// <para>
    /// <see cref="SubFlow{TOut,TMat,TClosed}.MergeSubstreams"/> and friends are typed to return the
    /// weak <see cref="IFlow{TOut,TMat}"/>, which loses the <see cref="Source{TOut,TMat}"/> /
    /// <see cref="Flow{TIn,TOut,TMat}"/> shape and forces a cast before the rest of the fluent
    /// builder can be used (see akka.net issue #5381). These extensions restore that shape: the
    /// substream's <c>TClosed</c> witness already distinguishes a <see cref="Source{TOut,TMat}"/>
    /// root (<see cref="IRunnableGraph{TMat}"/>) from a <see cref="Flow{TIn,TOut,TMat}"/> root
    /// (<see cref="Sink{TIn,TMat}"/>), and it survives every intermediate substream operator, so a
    /// single overloaded name resolves to the right concrete return type.
    /// </para>
    ///
    /// <para>
    /// The downcast is sound: the value produced by the merge is, at runtime, already the concrete
    /// <see cref="Source{TOut,TMat}"/> / <see cref="Flow{TIn,TOut,TMat}"/> — only its static type
    /// was widened to <see cref="IFlow{TOut,TMat}"/>.
    /// </para>
    /// </summary>
    public static class SubFlowMergeBackOperations
    {
        /// <summary>
        /// Flatten the sub-flows of a <see cref="Source{TOut,TMat}"/>-rooted substream back into the
        /// super-flow by merging, returning the concrete <see cref="Source{TOut,TMat}"/> so the fluent
        /// builder can continue without a cast.
        /// </summary>
        /// <typeparam name="TOut">The element type of the merged stream.</typeparam>
        /// <typeparam name="TMat">The materialized value type carried by the super-flow.</typeparam>
        /// <param name="flow">The substream produced by <c>GroupBy</c>/<c>SplitWhen</c>/<c>SplitAfter</c> on a <see cref="Source{TOut,TMat}"/>.</param>
        /// <param name="parallelism">
        /// The maximum number of substreams that may run concurrently. <see cref="int.MaxValue"/>
        /// (the default) performs an unbounded merge; <c>1</c> is equivalent to
        /// <see cref="SubFlow{TOut,TMat,TClosed}.ConcatSubstream"/>.
        /// </param>
        /// <returns>The merged super-flow as a concrete <see cref="Source{TOut,TMat}"/>.</returns>
        public static Source<TOut, TMat> MergeBack<TOut, TMat>(
            this SubFlow<TOut, TMat, IRunnableGraph<TMat>> flow, int parallelism = int.MaxValue) =>
            (Source<TOut, TMat>)flow.MergeSubstreamsWithParallelism(parallelism);

        /// <summary>
        /// Flatten the sub-flows of a <see cref="Flow{TIn,TOut,TMat}"/>-rooted substream back into the
        /// super-flow by merging, returning the concrete <see cref="Flow{TIn,TOut,TMat}"/> so the fluent
        /// builder can continue without a cast.
        /// </summary>
        /// <typeparam name="TIn">The input element type of the rooting <see cref="Flow{TIn,TOut,TMat}"/>.</typeparam>
        /// <typeparam name="TOut">The element type of the merged stream.</typeparam>
        /// <typeparam name="TMat">The materialized value type carried by the super-flow.</typeparam>
        /// <param name="flow">The substream produced by <c>GroupBy</c>/<c>SplitWhen</c>/<c>SplitAfter</c> on a <see cref="Flow{TIn,TOut,TMat}"/>.</param>
        /// <param name="parallelism">
        /// The maximum number of substreams that may run concurrently. <see cref="int.MaxValue"/>
        /// (the default) performs an unbounded merge; <c>1</c> is equivalent to
        /// <see cref="SubFlow{TOut,TMat,TClosed}.ConcatSubstream"/>.
        /// </param>
        /// <returns>The merged super-flow as a concrete <see cref="Flow{TIn,TOut,TMat}"/>.</returns>
        public static Flow<TIn, TOut, TMat> MergeBack<TIn, TOut, TMat>(
            this SubFlow<TOut, TMat, Sink<TIn, TMat>> flow, int parallelism = int.MaxValue) =>
            (Flow<TIn, TOut, TMat>)flow.MergeSubstreamsWithParallelism(parallelism);
    }
}
