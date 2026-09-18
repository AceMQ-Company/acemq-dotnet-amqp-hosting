// A worker that consumes orders, with everything this package does turned on: a
// connection and a topology from appsettings.json, a handler resolved from the container
// per message, a drain on shutdown, a health check, and OpenTelemetry.
//
// Run it against a broker on localhost:5672:
//
//     docker run -d --name acemq-example -p 5672:5672 rabbitmq:4-alpine
//     dotnet run --project examples/worker
//
// Then send it something — there is a publisher in the same process, on a timer, so it
// has work to do. Stop it with Ctrl-C and watch the drain in the log.

using AceMq.Amqp;
using AceMq.Amqp.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Worker;

var builder = Host.CreateApplicationBuilder(args);

// The host stops waiting after this; the drain in appsettings.json is deliberately
// shorter, so the drain is the deadline that actually applies.
builder.Services.Configure<HostOptions>(o => o.ShutdownTimeout = TimeSpan.FromSeconds(45));

builder.Services
    .AddAceMq(builder.Configuration.GetSection("acemq"))
    .AddConsumer<Order, OrderHandler>("orders.new");

builder.Services.AddSingleton<OrderPublisher>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<OrderPublisher>());

builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddAceMqInstrumentation().AddConsoleExporter())
    .WithMetrics(m => m.AddAceMqInstrumentation().AddConsoleExporter());

builder.Services.AddHealthChecks();

var host = builder.Build();
await host.RunAsync();

namespace Worker
{
    /// <summary>What travels on the queue. A record, encoded as JSON by default.</summary>
    public sealed record Order(string Id, decimal Total);

    /// <summary>
    /// An ordinary service. Constructor injection, scoped lifetime, resolved fresh for
    /// every message — nothing about it is specific to messaging except the interface.
    /// </summary>
    public sealed class OrderHandler : IMessageHandler<Order>
    {
        private readonly ILogger<OrderHandler> _log;

        public OrderHandler(ILogger<OrderHandler> log) => _log = log;

        public async Task<Ack> HandleAsync(
            IMessage<Order> message, CancellationToken cancellationToken)
        {
            _log.LogInformation(
                "order {Id} for {Total}, attempt {Attempt}",
                message.Payload.Id,
                message.Payload.Total,
                message.Attempt);

            // Work that takes long enough to still be running when Ctrl-C arrives, so the
            // drain has something to wait for. The token is passed through: it stays
            // uncancelled for the whole of a graceful drain, and is cancelled only once
            // the drain has overrun.
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);

            return Ack.Accept();
        }
    }

    /// <summary>Gives the worker something to do.</summary>
    public sealed class OrderPublisher : BackgroundService
    {
        private readonly IAceMqConnectionProvider _connections;
        private readonly ILogger<OrderPublisher> _log;

        public OrderPublisher(IAceMqConnectionProvider connections, ILogger<OrderPublisher> log)
        {
            _connections = connections;
            _log = log;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // The provider rather than AceMqConnection: this runs before the connection
            // host has necessarily finished, and awaiting is better than blocking.
            var connection = await _connections.GetAsync(stoppingToken);
            var publisher = connection.Publisher<Order>("orders", "order.created");

            var n = 0;
            while (!stoppingToken.IsCancellationRequested)
            {
                var order = new Order($"o-{++n}", n * 9.99m);
                var result = await publisher.SendAsync(order);
                _log.LogInformation("published {Id}, routed={Routed}", order.Id, result.Routed);

                try
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }
}
