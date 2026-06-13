//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2026 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Akka;
using Akka.Actor;
using Akka.Configuration;
using Akka.Streams;
using Akka.Streams.Dsl;

var settings = BroadcasterSettings.FromEnvironment();
var broadcaster = new BroadcastHubScaleService(settings);

await broadcaster.StartAsync();

var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://0.0.0.0:{settings.HttpPort.ToString(CultureInfo.InvariantCulture)}");

var app = builder.Build();

app.MapGet("/healthz", () => Results.Ok(new { status = "ok" }));
app.MapGet("/metrics", () => Results.Json(broadcaster.Snapshot()));
app.MapPost("/run", async (HttpContext context) =>
{
    var mode = context.Request.Query["mode"].FirstOrDefault() ?? settings.DefaultMode;
    var messageCountText = context.Request.Query["messageCount"].FirstOrDefault();
    var messageCount = messageCountText is null
        ? settings.DefaultMessageCount(mode)
        : int.Parse(messageCountText, CultureInfo.InvariantCulture);

    var result = await broadcaster.RunProducerAsync(mode, messageCount, context.RequestAborted);
    return Results.Json(result);
});

app.Lifetime.ApplicationStopping.Register(() => broadcaster.StopAsync().GetAwaiter().GetResult());

await app.RunAsync();

internal sealed record HubEvent(int Sequence, string Mode, long SentAtUnixTimeMilliseconds);

internal sealed record BroadcasterSettings(
    int TargetConsumers,
    int StreamBufferSize,
    int QueueBufferSize,
    int TcpPort,
    int HttpPort,
    string DefaultMode,
    int LockstepMessageCount,
    int FilterMessageCount,
    int TcpBacklog)
{
    public static BroadcasterSettings FromEnvironment()
    {
        var targetConsumers = ReadInt("TARGET_CONSUMERS", 20000);
        return new BroadcasterSettings(
            TargetConsumers: targetConsumers,
            StreamBufferSize: ReadInt("BUFFER_SIZE", 1024),
            QueueBufferSize: ReadInt("QUEUE_BUFFER_SIZE", 1024),
            TcpPort: ReadInt("TCP_PORT", 7000),
            HttpPort: ReadInt("HTTP_PORT", 8080),
            DefaultMode: Environment.GetEnvironmentVariable("MODE") ?? "filter",
            LockstepMessageCount: ReadInt("LOCKSTEP_MESSAGE_COUNT", 16),
            FilterMessageCount: ReadInt("FILTER_MESSAGE_COUNT", targetConsumers),
            TcpBacklog: ReadInt("TCP_BACKLOG", 32768));
    }

    public int DefaultMessageCount(string mode) =>
        string.Equals(mode, "lockstep", StringComparison.OrdinalIgnoreCase)
            ? LockstepMessageCount
            : FilterMessageCount;

    private static int ReadInt(string name, int fallback)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : fallback;
    }
}

internal sealed class BroadcastHubScaleService
{
    private readonly BroadcasterSettings _settings;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly ActorSystem _system;
    private readonly IMaterializer _materializer;
    private readonly ISourceQueueWithComplete<HubEvent> _queue;
    private readonly Source<HubEvent, NotUsed> _hubSource;
    private readonly TcpListener _listener;

    private long _acceptedConnections;
    private long _activeConnections;
    private long _materializedConsumers;
    private long _terminatedConsumers;
    private long _failedConsumers;
    private long _offeredMessages;
    private long _deliveredWrites;
    private long _failedWrites;
    private long _completedRuns;
    private long _lastRunStartedUnixTimeMilliseconds;
    private long _lastRunCompletedUnixTimeMilliseconds;
    private string _lastMode = "none";
    private string _lastRunStatus = "not-started";
    private Task? _acceptLoop;

    public BroadcastHubScaleService(BroadcasterSettings settings)
    {
        _settings = settings;
        _system = ActorSystem.Create("BroadcastHubScale", ConfigurationFactory.ParseString(@"
akka {
  loglevel = WARNING
  stdout-loglevel = WARNING
}
"));
        _materializer = _system.Materializer();
        (_queue, _hubSource) = Source
            .Queue<HubEvent>(_settings.QueueBufferSize, OverflowStrategy.Backpressure)
            .ToMaterialized(BroadcastHub.Sink<HubEvent>(_settings.TargetConsumers, _settings.StreamBufferSize), Keep.Both)
            .Run(_materializer);
        _listener = new TcpListener(IPAddress.Any, _settings.TcpPort);
    }

    public Task StartAsync()
    {
        _listener.Start(_settings.TcpBacklog);
        _acceptLoop = Task.Run(() => AcceptLoopAsync(_shutdown.Token));
        return Task.CompletedTask;
    }

    public async Task StopAsync()
    {
        _shutdown.Cancel();
        _listener.Stop();
        _queue.Complete();

        if (_acceptLoop is not null)
        {
            try
            {
                await _acceptLoop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (SocketException)
            {
            }
        }

        await _system.Terminate().ConfigureAwait(false);
    }

    public BroadcastMetrics Snapshot()
    {
        var process = Process.GetCurrentProcess();
        return new BroadcastMetrics(
            TargetConsumers: _settings.TargetConsumers,
            AcceptedConnections: Interlocked.Read(ref _acceptedConnections),
            ActiveConnections: Interlocked.Read(ref _activeConnections),
            MaterializedConsumers: Interlocked.Read(ref _materializedConsumers),
            TerminatedConsumers: Interlocked.Read(ref _terminatedConsumers),
            FailedConsumers: Interlocked.Read(ref _failedConsumers),
            OfferedMessages: Interlocked.Read(ref _offeredMessages),
            DeliveredWrites: Interlocked.Read(ref _deliveredWrites),
            FailedWrites: Interlocked.Read(ref _failedWrites),
            CompletedRuns: Interlocked.Read(ref _completedRuns),
            LastMode: Volatile.Read(ref _lastMode),
            LastRunStatus: Volatile.Read(ref _lastRunStatus),
            LastRunStartedUnixTimeMilliseconds: Interlocked.Read(ref _lastRunStartedUnixTimeMilliseconds),
            LastRunCompletedUnixTimeMilliseconds: Interlocked.Read(ref _lastRunCompletedUnixTimeMilliseconds),
            WorkingSetBytes: process.WorkingSet64,
            GcGen0Collections: GC.CollectionCount(0),
            GcGen1Collections: GC.CollectionCount(1),
            GcGen2Collections: GC.CollectionCount(2));
    }

    public async Task<RunResult> RunProducerAsync(string mode, int messageCount, CancellationToken cancellationToken)
    {
        if (messageCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(messageCount), messageCount, "Message count must be positive.");

        Volatile.Write(ref _lastMode, mode);
        Volatile.Write(ref _lastRunStatus, "running");
        Interlocked.Exchange(ref _lastRunStartedUnixTimeMilliseconds, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        var stopwatch = Stopwatch.StartNew();

        for (var i = 0; i < messageCount; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var result = await _queue
                .OfferAsync(new HubEvent(i, mode, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()), cancellationToken)
                .ConfigureAwait(false);

            if (result is QueueOfferResult.Enqueued)
            {
                Interlocked.Increment(ref _offeredMessages);
                continue;
            }

            Volatile.Write(ref _lastRunStatus, $"offer-failed:{result.GetType().Name}");
            throw new InvalidOperationException($"Source queue offer failed with {result.GetType().Name}.");
        }

        stopwatch.Stop();
        Interlocked.Increment(ref _completedRuns);
        Interlocked.Exchange(ref _lastRunCompletedUnixTimeMilliseconds, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        Volatile.Write(ref _lastRunStatus, "completed");

        return new RunResult(mode, messageCount, stopwatch.ElapsedMilliseconds, Snapshot());
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            var client = await _listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
            _ = Task.Run(() => HandleClientAsync(client, cancellationToken), cancellationToken);
        }
    }

    private async Task HandleClientAsync(TcpClient tcpClient, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _acceptedConnections);
        Interlocked.Increment(ref _activeConnections);

        using var client = tcpClient;
        await using var stream = client.GetStream();

        try
        {
            var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);
            var line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            var registration = ConsumerRegistration.Parse(line);
            var completion = MaterializeConsumerAsync(stream, registration, cancellationToken);

            await completion.ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception)
        {
            Interlocked.Increment(ref _failedConsumers);
        }
        finally
        {
            Interlocked.Decrement(ref _activeConnections);
        }
    }

    private Task MaterializeConsumerAsync(Stream stream, ConsumerRegistration registration, CancellationToken cancellationToken)
    {
        var source = string.Equals(registration.Mode, "filter", StringComparison.OrdinalIgnoreCase)
            ? _hubSource.Where(evt => evt.Sequence == registration.WatchedValue)
            : _hubSource;

        Interlocked.Increment(ref _materializedConsumers);

        var completion = source.RunWith(Sink.ForEachAsync<HubEvent>(1, async evt =>
        {
            var payload = Encoding.UTF8.GetBytes(evt.Sequence.ToString(CultureInfo.InvariantCulture) + "\n");
            try
            {
                await stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref _deliveredWrites);
            }
            catch
            {
                Interlocked.Increment(ref _failedWrites);
                throw;
            }
        }), _materializer);

        return completion.ContinueWith(task =>
        {
            Interlocked.Increment(ref _terminatedConsumers);
            if (task.IsFaulted)
                Interlocked.Increment(ref _failedConsumers);
        }, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
    }
}

internal sealed record ConsumerRegistration(string PodName, int ConnectionIndex, string Mode, int WatchedValue)
{
    public static ConsumerRegistration Parse(string? line)
    {
        if (string.IsNullOrWhiteSpace(line))
            throw new InvalidOperationException("Missing HELLO registration line.");

        var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length != 5 || !string.Equals(parts[0], "HELLO", StringComparison.Ordinal))
            throw new InvalidOperationException($"Invalid registration line: {line}");

        return new ConsumerRegistration(
            parts[1],
            int.Parse(parts[2], CultureInfo.InvariantCulture),
            parts[3],
            int.Parse(parts[4], CultureInfo.InvariantCulture));
    }
}

internal sealed record BroadcastMetrics(
    int TargetConsumers,
    long AcceptedConnections,
    long ActiveConnections,
    long MaterializedConsumers,
    long TerminatedConsumers,
    long FailedConsumers,
    long OfferedMessages,
    long DeliveredWrites,
    long FailedWrites,
    long CompletedRuns,
    string LastMode,
    string LastRunStatus,
    long LastRunStartedUnixTimeMilliseconds,
    long LastRunCompletedUnixTimeMilliseconds,
    long WorkingSetBytes,
    int GcGen0Collections,
    int GcGen1Collections,
    int GcGen2Collections);

internal sealed record RunResult(string Mode, int MessageCount, long ElapsedMilliseconds, BroadcastMetrics Metrics);
