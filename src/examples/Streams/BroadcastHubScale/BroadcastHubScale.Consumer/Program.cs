//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2026 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Globalization;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

var settings = ConsumerSettings.FromEnvironment();
using var shutdown = new CancellationTokenSource();

Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    shutdown.Cancel();
};

var metrics = new ConsumerMetrics(settings.PodName, settings.Mode, settings.ConnectionsPerPod, settings.ExpectedMessages);
_ = Enumerable
    .Range(0, settings.ConnectionsPerPod)
    .Select(index => RunConnectionAsync(settings, metrics, index, shutdown.Token))
    .ToArray();

var reporter = Task.Run(() => ReportLoopAsync(metrics, shutdown.Token), shutdown.Token);
var readyReporter = Task.Run(async () =>
{
    await metrics.WhenReadyAsync.WaitAsync(shutdown.Token).ConfigureAwait(false);
    Console.WriteLine(JsonSerializer.Serialize(metrics.Snapshot("ready")));
}, shutdown.Token);

try
{
    await Task.Delay(Timeout.InfiniteTimeSpan, shutdown.Token).ConfigureAwait(false);
}
catch (OperationCanceledException)
{
}

await readyReporter.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
await reporter.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

static async Task RunConnectionAsync(
    ConsumerSettings settings,
    ConsumerMetrics metrics,
    int connectionIndex,
    CancellationToken cancellationToken)
{
    var watchedValue = settings.WatchedValueBase + connectionIndex;
    var received = 0;

    while (!cancellationToken.IsCancellationRequested)
    {
        try
        {
            using var client = new TcpClient();
            await client.ConnectAsync(settings.BroadcasterHost, settings.BroadcasterPort, cancellationToken).ConfigureAwait(false);
            await using var stream = client.GetStream();
            var hello = Encoding.UTF8.GetBytes(
                $"HELLO {settings.PodName} {connectionIndex.ToString(CultureInfo.InvariantCulture)} {settings.Mode} {watchedValue.ToString(CultureInfo.InvariantCulture)}\n");

            await stream.WriteAsync(hello, cancellationToken).ConfigureAwait(false);
            metrics.ConnectionEstablished();

            using var reader = new StreamReader(stream, Encoding.UTF8, leaveOpen: true);
            while (received < settings.ExpectedMessages && !cancellationToken.IsCancellationRequested)
            {
                var line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
                if (line is null)
                    throw new IOException("Broadcaster closed the TCP stream.");

                received++;
                metrics.MessageReceived();
            }

            metrics.ConnectionReady();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            metrics.ConnectionFailed();
            Console.Error.WriteLine(JsonSerializer.Serialize(new
            {
                status = "connection-failed",
                settings.PodName,
                connectionIndex,
                watchedValue,
                error = ex.Message
            }));

            await Task.Delay(settings.ReconnectDelay, cancellationToken).ConfigureAwait(false);
        }
    }
}

static async Task ReportLoopAsync(ConsumerMetrics metrics, CancellationToken cancellationToken)
{
    while (!cancellationToken.IsCancellationRequested)
    {
        Console.WriteLine(JsonSerializer.Serialize(metrics.Snapshot("running")));
        await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
    }
}

internal sealed record ConsumerSettings(
    string BroadcasterHost,
    int BroadcasterPort,
    int ConnectionsPerPod,
    string PodName,
    string Mode,
    int WatchedValueBase,
    int ExpectedMessages,
    TimeSpan ReconnectDelay)
{
    public static ConsumerSettings FromEnvironment()
    {
        var connectionsPerPod = ReadInt("CONNECTIONS_PER_POD", 100);
        var podName = Environment.GetEnvironmentVariable("POD_NAME") ?? Environment.MachineName;
        var watchedValueBase = Environment.GetEnvironmentVariable("WATCHED_VALUE_BASE") is { Length: > 0 } explicitBase
            ? int.Parse(explicitBase, CultureInfo.InvariantCulture)
            : ParseOrdinal(podName) * connectionsPerPod;

        return new ConsumerSettings(
            BroadcasterHost: Environment.GetEnvironmentVariable("BROADCASTER_HOST") ?? "broadcast-hub-broadcaster",
            BroadcasterPort: ReadInt("BROADCASTER_PORT", 7000),
            ConnectionsPerPod: connectionsPerPod,
            PodName: podName,
            Mode: Environment.GetEnvironmentVariable("MODE") ?? "filter",
            WatchedValueBase: watchedValueBase,
            ExpectedMessages: ReadInt("EXPECTED_MESSAGES", 1),
            ReconnectDelay: TimeSpan.FromMilliseconds(ReadInt("RECONNECT_DELAY_MS", 1000)));
    }

    private static int ReadInt(string name, int fallback)
    {
        var value = Environment.GetEnvironmentVariable(name);
        return int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : fallback;
    }

    private static int ParseOrdinal(string podName)
    {
        var lastDash = podName.LastIndexOf('-');
        if (lastDash < 0)
            return 0;

        var suffix = podName[(lastDash + 1)..];
        return int.TryParse(suffix, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ordinal) ? ordinal : 0;
    }
}

internal sealed class ConsumerMetrics
{
    private readonly string _podName;
    private readonly string _mode;
    private readonly int _targetConnections;
    private readonly int _expectedMessages;
    private long _establishedConnections;
    private long _readyConnections;
    private long _failedConnections;
    private long _receivedMessages;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public ConsumerMetrics(string podName, string mode, int targetConnections, int expectedMessages)
    {
        _podName = podName;
        _mode = mode;
        _targetConnections = targetConnections;
        _expectedMessages = expectedMessages;
    }

    public void ConnectionEstablished() => Interlocked.Increment(ref _establishedConnections);
    public Task WhenReadyAsync => _ready.Task;

    public void ConnectionReady()
    {
        if (Interlocked.Increment(ref _readyConnections) == _targetConnections)
            _ready.TrySetResult();
    }
    public void ConnectionFailed() => Interlocked.Increment(ref _failedConnections);
    public void MessageReceived() => Interlocked.Increment(ref _receivedMessages);

    public object Snapshot(string status) => new
    {
        status,
        podName = _podName,
        mode = _mode,
        targetConnections = _targetConnections,
        expectedMessagesPerConnection = _expectedMessages,
        establishedConnections = Interlocked.Read(ref _establishedConnections),
        readyConnections = Interlocked.Read(ref _readyConnections),
        failedConnections = Interlocked.Read(ref _failedConnections),
        receivedMessages = Interlocked.Read(ref _receivedMessages)
    };
}
