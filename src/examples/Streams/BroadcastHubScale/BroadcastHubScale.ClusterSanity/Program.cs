//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2026 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

using System.Globalization;
using System.Text.Json;
using Akka.Actor;
using Akka.Cluster.Tools.PublishSubscribe;
using Akka.Configuration;

var settings = ClusterSanitySettings.FromEnvironment();
var config = ConfigurationFactory.ParseString(settings.ToHocon());
using var system = ActorSystem.Create("ClusterSanity", config);

var mediator = DistributedPubSub.Get(system).Mediator;
var metrics = new ClusterSanityMetrics(settings.PodName, settings.LocalSubscribers);

for (var i = 0; i < settings.LocalSubscribers; i++)
    system.ActorOf(Props.Create(() => new SanitySubscriber(metrics)), $"subscriber-{i.ToString(CultureInfo.InvariantCulture)}");

if (settings.PodOrdinal == 0)
{
    _ = Task.Run(async () =>
    {
        await Task.Delay(settings.PublishDelay).ConfigureAwait(false);
        mediator.Tell(new Publish(settings.Topic, $"cluster-sanity:{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture)}"));
        metrics.PublishSent();
    });
}

using var shutdown = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    shutdown.Cancel();
};

try
{
    while (!shutdown.IsCancellationRequested)
    {
        Console.WriteLine(JsonSerializer.Serialize(metrics.Snapshot()));
        await Task.Delay(TimeSpan.FromSeconds(5), shutdown.Token).ConfigureAwait(false);
    }
}
catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
{
}

await system.Terminate().ConfigureAwait(false);

internal sealed class SanitySubscriber : ReceiveActor
{
    private readonly ClusterSanityMetrics _metrics;

    public SanitySubscriber(ClusterSanityMetrics metrics)
    {
        _metrics = metrics;
        var mediator = DistributedPubSub.Get(Context.System).Mediator;
        mediator.Tell(new Subscribe(ClusterSanitySettings.TopicName, Self));

        Receive<SubscribeAck>(ack =>
        {
            if (ack.Subscribe.Topic == ClusterSanitySettings.TopicName && ack.Subscribe.Ref.Equals(Self))
                _metrics.SubscribeAcked();
        });
        Receive<string>(_ => _metrics.MessageReceived());
    }
}

internal sealed record ClusterSanitySettings(
    string PodName,
    int PodOrdinal,
    string PodIp,
    string Namespace,
    int RemotePort,
    int Replicas,
    int TotalSubscribers,
    int LocalSubscribers,
    TimeSpan PublishDelay,
    string Topic)
{
    public const string TopicName = "broadcast-hub-scale-sanity";

    public static ClusterSanitySettings FromEnvironment()
    {
        var podName = Environment.GetEnvironmentVariable("POD_NAME") ?? Environment.MachineName;
        var ordinal = ParseOrdinal(podName);
        var replicas = ReadInt("CLUSTER_REPLICAS", 3);
        var totalSubscribers = ReadInt("TOTAL_SUBSCRIBERS", 20000);
        var baseSubscribers = totalSubscribers / replicas;
        var remainder = totalSubscribers % replicas;
        var localSubscribers = baseSubscribers + (ordinal < remainder ? 1 : 0);

        return new ClusterSanitySettings(
            PodName: podName,
            PodOrdinal: ordinal,
            PodIp: Environment.GetEnvironmentVariable("POD_IP") ?? "127.0.0.1",
            Namespace: Environment.GetEnvironmentVariable("POD_NAMESPACE") ?? "akka-7253",
            RemotePort: ReadInt("REMOTE_PORT", 4053),
            Replicas: replicas,
            TotalSubscribers: totalSubscribers,
            LocalSubscribers: localSubscribers,
            PublishDelay: TimeSpan.FromSeconds(ReadInt("PUBLISH_DELAY_SECONDS", 60)),
            Topic: TopicName);
    }

    public string ToHocon()
    {
        var seedNodes = string.Join(", ", Enumerable.Range(0, Replicas).Select(i =>
            $"\"akka.tcp://ClusterSanity@cluster-sanity-{i.ToString(CultureInfo.InvariantCulture)}.cluster-sanity.{Namespace}.svc.cluster.local:{RemotePort.ToString(CultureInfo.InvariantCulture)}\""));

        return $$"""
akka {
  loglevel = WARNING
  stdout-loglevel = WARNING
  actor.provider = cluster
  extensions = ["Akka.Cluster.Tools.PublishSubscribe.DistributedPubSubExtensionProvider,Akka.Cluster.Tools"]
  remote.dot-netty.tcp {
    hostname = "{{PodIp}}"
    port = {{RemotePort.ToString(CultureInfo.InvariantCulture)}}
  }
  cluster {
    seed-nodes = [{{seedNodes}}]
    roles = ["sanity"]
  }
}
""";
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

internal sealed class ClusterSanityMetrics
{
    private readonly string _podName;
    private readonly int _localSubscribers;
    private long _subscribeAcks;
    private long _receivedMessages;
    private long _publishedMessages;

    public ClusterSanityMetrics(string podName, int localSubscribers)
    {
        _podName = podName;
        _localSubscribers = localSubscribers;
    }

    public void SubscribeAcked() => Interlocked.Increment(ref _subscribeAcks);
    public void MessageReceived() => Interlocked.Increment(ref _receivedMessages);
    public void PublishSent() => Interlocked.Increment(ref _publishedMessages);

    public object Snapshot() => new
    {
        status = "cluster-sanity",
        podName = _podName,
        localSubscribers = _localSubscribers,
        subscribeAcks = Interlocked.Read(ref _subscribeAcks),
        receivedMessages = Interlocked.Read(ref _receivedMessages),
        publishedMessages = Interlocked.Read(ref _publishedMessages)
    };
}
