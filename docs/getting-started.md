# Getting started

A worker that consumes a queue, from nothing, in about five minutes.

## A broker

```bash
docker run -d --name acemq-dev -p 5672:5672 rabbitmq:4-alpine
```

## The packages

AceMQ is not on nuget.org before 1.0, so the feed it does live on has to be named. A
`NuGet.config` beside the solution is the usual place:

```xml
<?xml version="1.0" encoding="utf-8"?>
<configuration>
  <packageSources>
    <clear />
    <add key="nuget.org" value="https://api.nuget.org/v3/index.json" protocolVersion="3" />
    <add key="acemq" value="https://acemq.org/nuget/index.json" protocolVersion="3" />
  </packageSources>
  <packageSourceMapping>
    <packageSource key="acemq">
      <package pattern="AceMq.*" />
    </packageSource>
    <packageSource key="nuget.org">
      <package pattern="*" />
    </packageSource>
  </packageSourceMapping>
</configuration>
```

The source mapping is not decoration. Without it, an unauthenticated feed in the list can
answer for any package id; with it, the AceMQ feed can only ever serve `AceMq.*`.

```bash
dotnet add package AceMq.Amqp.Hosting --version 0.1.0
```

The version is pinned because this package is `0.x`, and `0.y.z` is outside what semver
promises: the configuration keys and the builder surface may change in any release. Pin
until 1.0.

That brings `AceMq.Amqp` 0.7.2 and `AceMq.Amqp.RabbitMq` 0.7.2 with it — the released
library this version is built and tested against, and the minimum the nuspec asks for. An
application already on a later library keeps it. The transport ships in the box
on purpose — transports in this library are registered by hand rather than discovered by
scanning, and a package that configures a connection from a URL and then cannot open one
would be a poor trade for a saved dependency.

## The worker

```csharp
using AceMq.Amqp;
using AceMq.Amqp.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services
    .AddAceMq(builder.Configuration.GetSection("acemq"))
    .AddConsumer<Order, OrderHandler>("orders.new");

await builder.Build().RunAsync();

public sealed record Order(string Id, decimal Total);

public sealed class OrderHandler : IMessageHandler<Order>
{
    private readonly ILogger<OrderHandler> _log;

    public OrderHandler(ILogger<OrderHandler> log) => _log = log;

    public async Task<Ack> HandleAsync(IMessage<Order> message, CancellationToken cancellationToken)
    {
        _log.LogInformation("order {Id} for {Total}", message.Payload.Id, message.Payload.Total);
        await Task.CompletedTask;
        return Ack.Accept();
    }
}
```

`OrderHandler` is an ordinary service. It is registered for you, scoped, and resolved from
a fresh scope for every message — so a `DbContext` injected into it behaves exactly as it
does in a controller.

## The configuration

```json
{
  "acemq": {
    "url": "amqp://guest:guest@localhost:5672",
    "topology": {
      "exchanges": [{ "name": "orders", "type": "topic" }],
      "queues": [{ "name": "orders.new", "deadLetter": true }],
      "bindings": [
        { "queue": "orders.new", "exchange": "orders", "routingKey": "order.created" }
      ]
    }
  }
}
```

The topology is declared at startup, before any consumer starts. Nothing is declared unless
something is written here — see [topology](topology.md).

## Sending something

Inject the connection and publish:

```csharp
public sealed class Orders
{
    private readonly AceMqConnection _mq;

    public Orders(AceMqConnection mq) => _mq = mq;

    public Task Place(Order order) =>
        _mq.Publisher<Order>("orders", "order.created").SendAsync(order);
}
```

Confirms are on by default, so a `SendAsync` that returns has been accepted by the broker.
More in [publishing](publishing.md).

## Then

- [Configuration](configuration.md) — every setting, and what each one costs
- [Startup and shutdown](lifecycle.md) — what a drain finishes and what it does not
- [Health checks](health.md) — and why a blocked broker is not a reason to restart
- The [`examples/worker`](https://github.com/AceMQ-Company/acemq-dotnet-amqp-hosting/tree/main/examples/worker)
  project in the repository is all of the above in one runnable program.
