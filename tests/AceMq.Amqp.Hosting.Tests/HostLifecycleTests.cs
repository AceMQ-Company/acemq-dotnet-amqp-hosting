using AceMq.Amqp.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;

namespace AceMq.Amqp.Hosting.Tests;

// AceMq.Amqp has a HealthStatus of its own, and this file is in a namespace under
// AceMq.Amqp, so the enclosing namespace would win over a using directive at the
// top of the file. Declared inside the namespace, the alias wins instead. An
// application outside the AceMq.Amqp namespace never meets this.
using HealthStatus = Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus;

/// <summary>
/// The host, end to end, over the library's in-memory transport.
/// </summary>
/// <remarks>
/// <para>The in-memory transport is a real transport — it declares, binds, routes and
/// redelivers — so everything this package does above it is exercised here: the ordering of
/// the two hosted services, the scope per message, the drain, and the cancellation that
/// comes after a drain that overran.</para>
///
/// <para>What it cannot prove is that any of it survives a socket, a broker that blocks, or
/// a prefetch window full of deliveries. That is what the integration suite is for, and
/// these tests are not a substitute for it. They are the half that runs in a second and
/// never flakes.</para>
/// </remarks>
public class HostLifecycleTests
{
    private sealed record Order(string Id);

    private static IHost Build(
        string broker,
        Action<IAceMqBuilder> consumers,
        Action<AceMqOptions>? configure = null,
        Action<IServiceCollection>? services = null)
    {
        return new HostBuilder()
            .ConfigureAppConfiguration(c => c.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["acemq:url"] = "memory://" + broker,
                ["acemq:listener:shutdownTimeout"] = "00:00:05",
                ["acemq:topology:queues:0:name"] = "orders.new",
                ["acemq:topology:exchanges:0:name"] = "orders",
                ["acemq:topology:exchanges:0:type"] = "topic",
                ["acemq:topology:bindings:0:queue"] = "orders.new",
                ["acemq:topology:bindings:0:exchange"] = "orders",
                ["acemq:topology:bindings:0:routingKey"] = "order.created",
            }))
            .ConfigureServices((context, s) =>
            {
                services?.Invoke(s);
                consumers(s.AddAceMq(context.Configuration.GetSection("acemq"), configure));
            })
            .Build();
    }

    [Fact]
    public async Task A_host_connects_declares_consumes_and_stops()
    {
        var handled = new TaskCompletionSource<Order>();

        using var host = Build(
            nameof(A_host_connects_declares_consumes_and_stops),
            b => b.AddConsumer<Order>("orders.new", (message, _) =>
            {
                handled.TrySetResult(message.Payload);
                return Task.FromResult(Ack.Accept());
            }));

        await host.StartAsync();

        var connection = host.Services.GetRequiredService<AceMqConnection>();
        Assert.True(connection.IsOpen);
        // The topology was applied by the connection host, before any consumer started.
        Assert.True(await connection.QueueExistsAsync("orders.new"));

        await connection.Publisher<Order>("orders", "order.created").SendAsync(new Order("o-1"));

        var order = await handled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("o-1", order.Id);

        await host.StopAsync();
    }

    [Fact]
    public async Task A_handler_is_resolved_from_a_scope_of_its_own_per_message()
    {
        using var host = Build(
            nameof(A_handler_is_resolved_from_a_scope_of_its_own_per_message),
            b => b.AddConsumer<Order, ScopeRecordingHandler>("orders.new"),
            services: s => s.AddScoped<ScopeMarker>());

        await host.StartAsync();

        var connection = host.Services.GetRequiredService<AceMqConnection>();
        var publisher = connection.Publisher<Order>("orders", "order.created");
        await publisher.SendAsync(new Order("o-1"));
        await publisher.SendAsync(new Order("o-2"));

        await ScopeRecordingHandler.Two.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Two messages, two scopes, so two distinct scoped instances. One shared instance
        // would mean a DbContext shared between messages, which is the bug this design
        // exists to prevent.
        Assert.Equal(2, ScopeRecordingHandler.Markers.Distinct().Count());

        await host.StopAsync();
    }

    [Fact]
    public async Task A_handler_in_flight_finishes_before_shutdown_returns()
    {
        var entered = new TaskCompletionSource<bool>();
        var finished = false;

        using var host = Build(
            nameof(A_handler_in_flight_finishes_before_shutdown_returns),
            b => b.AddConsumer<Order>("orders.new", async (_, _) =>
            {
                entered.TrySetResult(true);
                // Longer than the poll interval of the drain, shorter than its budget.
                await Task.Delay(TimeSpan.FromSeconds(1));
                finished = true;
                return Ack.Accept();
            }));

        await host.StartAsync();

        var connection = host.Services.GetRequiredService<AceMqConnection>();
        await connection.Publisher<Order>("orders", "order.created").SendAsync(new Order("o-1"));

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(finished);

        // The guarantee: StopAsync blocks on the handler rather than tearing it off. A
        // handler torn off halfway has already applied whatever side effects it reached,
        // and the message comes back afterwards to have them applied again.
        await host.StopAsync();

        Assert.True(finished);
        Assert.Equal(0, connection.InFlight);
    }

    [Fact]
    public async Task A_drain_that_overruns_gives_up_and_cancels_the_handlers()
    {
        var entered = new TaskCompletionSource<bool>();
        var cancelled = new TaskCompletionSource<bool>();

        using var host = Build(
            nameof(A_drain_that_overruns_gives_up_and_cancels_the_handlers),
            b => b.AddConsumer<Order>("orders.new", async (_, token) =>
            {
                entered.TrySetResult(true);
                try
                {
                    await Task.Delay(TimeSpan.FromMinutes(5), token);
                }
                catch (OperationCanceledException)
                {
                    cancelled.TrySetResult(true);
                    throw;
                }

                return Ack.Accept();
            }),
            configure: o => o.Listener.ShutdownTimeout = TimeSpan.FromMilliseconds(500));

        await host.StartAsync();

        var connection = host.Services.GetRequiredService<AceMqConnection>();
        await connection.Publisher<Order>("orders", "order.created").SendAsync(new Order("o-1"));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var clock = System.Diagnostics.Stopwatch.StartNew();
        await host.StopAsync();
        clock.Stop();

        // It gave up rather than waiting out the five minutes...
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(30), $"took {clock.Elapsed}");
        // ...and cancelled the handler, so the delivery goes back to the broker rather
        // than being abandoned against a connection that is about to close.
        Assert.True(await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task The_handler_token_is_not_the_hosts_stopping_token()
    {
        // The distinction the drain depends on. A handler given the host's stopping token
        // sees it cancelled the moment shutdown begins, aborts, and the drain it was meant
        // to survive is the thing that killed it.
        CancellationToken seen = default;
        var entered = new TaskCompletionSource<bool>();

        using var host = Build(
            nameof(The_handler_token_is_not_the_hosts_stopping_token),
            b => b.AddConsumer<Order>("orders.new", async (_, token) =>
            {
                seen = token;
                entered.TrySetResult(true);
                await Task.Delay(TimeSpan.FromMilliseconds(300), CancellationToken.None);
                Assert.False(token.IsCancellationRequested);
                return Ack.Accept();
            }));

        await host.StartAsync();

        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
        var connection = host.Services.GetRequiredService<AceMqConnection>();
        await connection.Publisher<Order>("orders", "order.created").SendAsync(new Order("o-1"));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.False(seen.IsCancellationRequested);
        Assert.NotEqual(lifetime.ApplicationStopping, seen);

        await host.StopAsync();
        Assert.True(seen.IsCancellationRequested);
    }

    [Fact]
    public async Task A_consumer_with_auto_startup_off_waits_to_be_asked()
    {
        using var host = Build(
            nameof(A_consumer_with_auto_startup_off_waits_to_be_asked),
            b => b.AddConsumer<Order>(
                "orders.new",
                (_, _) => Task.FromResult(Ack.Accept()),
                name: "orders",
                configure: r => r.AutoStartup = false));

        await host.StartAsync();

        var consumers = host.Services.GetRequiredService<AceMqConsumerHost>();
        Assert.Empty(consumers.Running);

        await consumers.StartAsync("orders");
        Assert.Contains("orders", consumers.Running);

        await host.StopAsync();
    }

    [Fact]
    public async Task Health_is_healthy_when_connected_and_unhealthy_while_draining()
    {
        using var host = Build(
            nameof(Health_is_healthy_when_connected_and_unhealthy_while_draining),
            b => b.AddConsumer<Order>("orders.new", (_, _) => Task.FromResult(Ack.Accept())));

        var check = host.Services.GetRequiredService<AceMqHealthCheck>();
        var context = new HealthCheckContext();

        // Before the host starts there is no connection, and an application that cannot do
        // its work should not claim it can.
        var before = await check.CheckHealthAsync(context);
        Assert.Equal(HealthStatus.Unhealthy, before.Status);
        Assert.Equal("not connected", before.Description);

        await host.StartAsync();

        var up = await check.CheckHealthAsync(context);
        Assert.Equal(HealthStatus.Healthy, up.Status);
        Assert.Equal("in-memory", up.Data["transport"]);
        Assert.Equal(true, up.Data["open"]);
        Assert.Equal(false, up.Data["blocked"]);
        Assert.Equal(1, up.Data["consumers"]);

        await host.StopAsync();

        // Draining is the one case where unhealthy is right: an instance that has been
        // told to stop should leave the rotation before it starts refusing work.
        var draining = await check.CheckHealthAsync(context);
        Assert.Equal(HealthStatus.Unhealthy, draining.Status);
        Assert.Equal("draining", draining.Description);
    }

    private sealed class ScopeMarker
    {
        public Guid Id { get; } = Guid.NewGuid();
    }

    private sealed class ScopeRecordingHandler : IMessageHandler<Order>
    {
        internal static readonly List<Guid> Markers = new();
        internal static readonly TaskCompletionSource<bool> Two = new();

        private readonly ScopeMarker _marker;

        public ScopeRecordingHandler(ScopeMarker marker) => _marker = marker;

        public Task<Ack> HandleAsync(IMessage<Order> message, CancellationToken cancellationToken)
        {
            lock (Markers)
            {
                Markers.Add(_marker.Id);
                if (Markers.Count >= 2) Two.TrySetResult(true);
            }

            return Task.FromResult(Ack.Accept());
        }
    }
}
