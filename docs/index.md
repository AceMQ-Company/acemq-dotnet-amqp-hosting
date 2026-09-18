# AceMQ hosting for .NET

Dependency injection and hosted-service integration for
[AceMQ for .NET](https://acemq.org/acemq-dotnet-amqp/): one `AddAceMq` call, handlers
resolved from the container, consumers that start after the host is built and drain when
it stops, and a health check that knows the difference between a broker that is down and a
broker that is busy.

```csharp
builder.Services
    .AddAceMq(builder.Configuration.GetSection("acemq"))
    .AddConsumer<Order, OrderHandler>("orders.new");
```

```json
{
  "acemq": {
    "url": "amqp://localhost:5672",
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

That is the whole surface for the common case.

## The pages

| | |
|---|---|
| **Start here** | [Getting started](getting-started.md) |
| **Reference** | [Configuration](configuration.md) — every setting under `acemq` |
| **Usage** | [Handlers and consumers](handlers.md) · [Topology](topology.md) · [Publishing](publishing.md) · [Testing](testing.md) |
| **Operations** | [Startup and shutdown](lifecycle.md) · [Health checks](health.md) · [Metrics and tracing](observability.md) |
| **Support** | [Enterprise support](https://acemq.com) · [Licence](licence.md) |

## What this is, and what it is not

It is the .NET analogue of the
[Spring Boot starter](https://acemq.org/acemq-java-amqp-spring-boot-starter/): the same
job, done the way the target framework does things. Where the starter scans for an
annotation, this resolves an interface from the container, because that is what a .NET
application already understands and because it means a handler is an ordinary service with
ordinary constructor injection.

It is **not** a second messaging library. Everything it configures is
[AceMq.Amqp](https://acemq.org/acemq-dotnet-amqp/), and an application that outgrows what
is configurable here drops to the library directly without leaving anything behind — the
connection it hands you is the library's own `AceMqConnection`, with the library's whole
surface on it.

It is on **its own version line**, starting at 0.1.0, because it tracks two release trains:
AceMQ's and Microsoft.Extensions'. A change in either can force a release here, and a
shared version number would only be able to say that one of them moved.
