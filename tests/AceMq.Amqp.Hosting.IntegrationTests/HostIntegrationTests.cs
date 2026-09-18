using AceMq.Amqp.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace AceMq.Amqp.Hosting.IntegrationTests;

using HealthStatus = Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus;

/// <summary>
/// A host, a real broker, and a shutdown.
/// </summary>
/// <remarks>
/// <para>The unit suite proves the same behaviours over the in-memory transport, where
/// there is no socket, no prefetch window and no broker. These prove them over a connection
/// that can be closed underneath the process, which is the only way to know the drain is a
/// drain and not an artefact of a transport that has nothing to drain.</para>
///
/// <para>No skip path. A suite that quietly does nothing when the broker is absent reports
/// a green tick for exactly the thing nobody wants unverified. Set
/// <c>ACEMQ_TEST_URL</c> to point it somewhere else.</para>
/// </remarks>
[Collection("broker")]
public class HostIntegrationTests
{
    private static string Url =>
        Environment.GetEnvironmentVariable("ACEMQ_TEST_URL")
        ?? "amqp://guest:guest@localhost:5721";

    private sealed record Order(string Id);

    private static IHost Build(
        string suffix,
        Action<IAceMqBuilder> consumers,
        Action<AceMqOptions>? configure = null)
    {
        var exchange = "hosting.it." + suffix;
        var queue = exchange + ".q";

        return new HostBuilder()
            .ConfigureAppConfiguration(c => c.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["acemq:url"] = Url,
                ["acemq:clientName"] = "hosting-it-" + suffix,
                ["acemq:listener:shutdownTimeout"] = "00:00:10",
                ["acemq:topology:exchanges:0:name"] = exchange,
                ["acemq:topology:exchanges:0:type"] = "topic",
                ["acemq:topology:queues:0:name"] = queue,
                ["acemq:topology:queues:0:type"] = "Classic",
                ["acemq:topology:bindings:0:queue"] = queue,
                ["acemq:topology:bindings:0:exchange"] = exchange,
                ["acemq:topology:bindings:0:routingKey"] = "order.created",
            }))
            .ConfigureServices((context, s) =>
                consumers(s.AddAceMq(context.Configuration.GetSection("acemq"), configure)))
            .Build();
    }

    private static string Exchange(string suffix) => "hosting.it." + suffix;

    private static string Queue(string suffix) => Exchange(suffix) + ".q";

    [Fact]
    public async Task A_host_declares_its_topology_consumes_a_message_and_stops_cleanly()
    {
        const string suffix = "roundtrip";
        var handled = new TaskCompletionSource<Order>();

        using var host = Build(suffix, b => b.AddConsumer<Order>(Queue(suffix), (message, _) =>
        {
            handled.TrySetResult(message.Payload);
            return Task.FromResult(Ack.Accept());
        }));

        await host.StartAsync();

        var connection = host.Services.GetRequiredService<AceMqConnection>();
        Assert.Equal("rabbitmq", connection.TransportName);
        Assert.True(await connection.QueueExistsAsync(Queue(suffix)));

        var result = await connection
            .Publisher<Order>(Exchange(suffix), "order.created")
            .SendAsync(new Order("o-1"));
        // Confirms are on by default, so a send that returned was accepted by the broker.
        Assert.True(result.Routed);

        var order = await handled.Task.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal("o-1", order.Id);

        await host.StopAsync();
        // The connection outlives the consumers and is disposed with the host.
        Assert.True(connection.IsOpen);

        host.Dispose();
        Assert.False(connection.IsOpen);

        await Cleanup(suffix);
    }

    [Fact]
    public async Task A_handler_in_flight_finishes_and_its_message_is_acknowledged()
    {
        // The test most worth having. A handler torn off mid-flight leaves a message
        // unacknowledged and its side effects half applied; the broker redelivers it and
        // the half that ran runs again. This asserts both halves of the guarantee: the
        // handler finished, and the broker has nothing left to redeliver.
        const string suffix = "drain";
        var entered = new TaskCompletionSource<bool>();
        var finished = false;

        using var host = Build(suffix, b => b.AddConsumer<Order>(Queue(suffix), async (_, _) =>
        {
            entered.TrySetResult(true);
            await Task.Delay(TimeSpan.FromSeconds(2));
            finished = true;
            return Ack.Accept();
        }));

        await host.StartAsync();

        var connection = host.Services.GetRequiredService<AceMqConnection>();
        await connection.Publisher<Order>(Exchange(suffix), "order.created")
            .SendAsync(new Order("o-1"));

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.False(finished);

        var clock = System.Diagnostics.Stopwatch.StartNew();
        await host.StopAsync();
        clock.Stop();

        Assert.True(finished, "the handler was torn off instead of finished");
        Assert.Equal(0, connection.InFlight);
        // It waited, rather than returning immediately and leaving the handler to run into
        // a closing connection.
        Assert.True(clock.Elapsed > TimeSpan.FromMilliseconds(500), $"returned after {clock.Elapsed}");

        // Acknowledged, not merely finished: nothing is left on the queue and nothing is
        // unacknowledged waiting to come back.
        Assert.Equal(0, await connection.MessageCountAsync(Queue(suffix)));

        host.Dispose();
        await Cleanup(suffix);
    }

    [Fact]
    public async Task A_message_whose_handler_did_not_finish_comes_back_rather_than_being_lost()
    {
        // The other side of the drain: when it overruns, the messages still in a handler
        // are not acknowledged, so the broker has them again after the process goes. One
        // extra attempt, no lost message.
        const string suffix = "overrun";
        var entered = new TaskCompletionSource<bool>();

        using var host = Build(
            suffix,
            b => b.AddConsumer<Order>(Queue(suffix), async (_, token) =>
            {
                entered.TrySetResult(true);
                await Task.Delay(TimeSpan.FromMinutes(5), token);
                return Ack.Accept();
            }),
            configure: o => o.Listener.ShutdownTimeout = TimeSpan.FromSeconds(1));

        await host.StartAsync();

        var connection = host.Services.GetRequiredService<AceMqConnection>();
        await connection.Publisher<Order>(Exchange(suffix), "order.created")
            .SendAsync(new Order("o-1"));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));

        var clock = System.Diagnostics.Stopwatch.StartNew();
        await host.StopAsync();
        clock.Stop();
        host.Dispose();

        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(30), $"took {clock.Elapsed}");

        // A second connection, because the first one is gone with the host. The broker
        // requeues what was never acknowledged as soon as the channel closes.
        using var after = await Connect();
        var count = 0L;
        for (var i = 0; i < 50 && count == 0; i++)
        {
            count = await after.MessageCountAsync(Queue(suffix));
            if (count == 0) await Task.Delay(200);
        }

        Assert.Equal(1, count);

        await after.DeleteQueueAsync(Queue(suffix));
        await after.DeleteExchangeAsync(Exchange(suffix));
    }

    [Fact]
    public async Task A_handler_type_is_resolved_per_message_from_the_container()
    {
        const string suffix = "scope";
        using var host = Build(suffix, b => b.AddConsumer<Order, CountingHandler>(Queue(suffix)));

        await host.StartAsync();

        var connection = host.Services.GetRequiredService<AceMqConnection>();
        var publisher = connection.Publisher<Order>(Exchange(suffix), "order.created");
        await publisher.SendAsync(new Order("o-1"));
        await publisher.SendAsync(new Order("o-2"));
        await publisher.SendAsync(new Order("o-3"));

        await CountingHandler.Three.Task.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(3, CountingHandler.Instances.Count);
        Assert.Equal(3, CountingHandler.Instances.Distinct().Count());

        await host.StopAsync();
        host.Dispose();
        await Cleanup(suffix);
    }

    [Fact]
    public async Task Health_reports_the_connection_and_the_consumers()
    {
        const string suffix = "health";
        using var host = Build(
            suffix,
            b => b.AddConsumer<Order>(Queue(suffix), (_, _) => Task.FromResult(Ack.Accept())));

        var check = host.Services.GetRequiredService<AceMqHealthCheck>();
        Assert.Equal(
            HealthStatus.Unhealthy,
            (await check.CheckHealthAsync(new HealthCheckContext())).Status);

        await host.StartAsync();

        var report = await check.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Healthy, report.Status);
        Assert.Equal("rabbitmq", report.Data["transport"]);
        Assert.Equal(false, report.Data["blocked"]);
        Assert.Equal(1, report.Data["consumers"]);

        await host.StopAsync();
        Assert.Equal(
            HealthStatus.Unhealthy,
            (await check.CheckHealthAsync(new HealthCheckContext())).Status);

        host.Dispose();
        await Cleanup(suffix);
    }

    [Fact]
    public async Task The_retry_ladder_is_declared_when_a_consumer_asks_for_it()
    {
        const string suffix = "retry";
        using var host = Build(
            suffix,
            b => b.AddConsumer<Order>(Queue(suffix), (_, _) => Task.FromResult(Ack.Accept())),
            configure: o =>
            {
                o.Listener.Retry.Enabled = true;
                o.Listener.Retry.MaxAttempts = 3;
                o.Listener.Retry.InitialDelay = TimeSpan.FromSeconds(1);
                o.Listener.Retry.MaxDelay = TimeSpan.FromMinutes(1);
                // Everything waits on the broker rather than in the process, which is what
                // keeps a drain from having to sit through a backoff.
                o.Listener.Retry.BrokerWaitThreshold = TimeSpan.FromMilliseconds(1);
            });

        await host.StartAsync();

        var connection = host.Services.GetRequiredService<AceMqConnection>();
        var ladder = RetryLadder.For(
            Queue(suffix),
            RetryPolicy.Exponential(3, TimeSpan.FromSeconds(1), 2.0, TimeSpan.FromMinutes(1))
                .WaitInBrokerFrom(TimeSpan.FromMilliseconds(1)));

        Assert.NotEmpty(ladder.Queues);
        foreach (var rung in ladder.Queues)
        {
            Assert.True(
                await connection.QueueExistsAsync(rung),
                $"the rung queue {rung} was not declared; a retry that should wait on the " +
                "broker would wait in the process instead, and a drain would sit through it");
        }

        await host.StopAsync();
        host.Dispose();

        using var cleanup = await Connect();
        foreach (var rung in ladder.Queues) await cleanup.DeleteQueueAsync(rung);
        await cleanup.DeleteQueueAsync(ladder.DeadLetterQueue);
        await cleanup.DeleteQueueAsync(ladder.ParkedQueue);
        await cleanup.DeleteQueueAsync(Queue(suffix));
        await cleanup.DeleteExchangeAsync(Exchange(suffix));
        await cleanup.DeleteExchangeAsync(RetryLadder.RetryExchange);
    }

    private static async Task<AceMqConnection> Connect()
    {
        Transports.Register(new AceMq.Amqp.RabbitMq.RabbitMqTransport());
        return await AceMqConnection.ConnectAsync(Url);
    }

    private static async Task Cleanup(string suffix)
    {
        using var connection = await Connect();
        await connection.DeleteQueueAsync(Queue(suffix));
        await connection.DeleteExchangeAsync(Exchange(suffix));
    }

    private sealed class CountingHandler : IMessageHandler<Order>
    {
        internal static readonly List<Guid> Instances = new();
        internal static readonly TaskCompletionSource<bool> Three = new();

        private readonly Guid _id = Guid.NewGuid();

        public Task<Ack> HandleAsync(IMessage<Order> message, CancellationToken cancellationToken)
        {
            lock (Instances)
            {
                Instances.Add(_id);
                if (Instances.Count >= 3) Three.TrySetResult(true);
            }

            return Task.FromResult(Ack.Accept());
        }
    }
}

/// <summary>
/// One broker, one test at a time. The tests declare and delete topology by name, and two
/// of them running at once would be two processes agreeing on a queue and disagreeing about
/// whether it should exist.
/// </summary>
[CollectionDefinition("broker", DisableParallelization = true)]
public class BrokerCollection
{
}
