# acemq-dotnet-amqp-hosting

[![ci](https://github.com/AceMQ-Company/acemq-dotnet-amqp-hosting/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-dotnet-amqp-hosting/actions/workflows/ci.yml)
[![authorship guard](https://github.com/AceMQ-Company/acemq-dotnet-amqp-hosting/actions/workflows/attribution-guard.yml/badge.svg?branch=main)](https://github.com/AceMQ-Company/acemq-dotnet-amqp-hosting/actions/workflows/attribution-guard.yml)
[![version](https://img.shields.io/badge/version-0.1.0-blue)](https://github.com/AceMQ-Company/acemq-dotnet-amqp-hosting/releases)
[![packages](https://img.shields.io/badge/packages-acemq.org%2Fnuget-blue)](https://acemq.org/nuget/index.json)
[![docs](https://img.shields.io/badge/docs-acemq.org-blue)](https://acemq.org/acemq-dotnet-amqp-hosting/)
[![license](https://img.shields.io/badge/license-Apache--2.0-green)](LICENSE)
[![targets](https://img.shields.io/badge/targets-netstandard2.0%20%7C%20net8.0-orange)](#requirements)

Dependency injection and hosted-service integration for
[acemq-dotnet-amqp](https://github.com/AceMQ-Company/acemq-dotnet-amqp): one connection, a
declared topology, handlers resolved from the container, consumers that drain on shutdown,
and a health check — configured from `appsettings.json` and nothing else.

> **Status: `0.1.0`, the first release.** 52 unit tests over the library's in-memory
> transport and 6 integration tests against RabbitMQ 4, plus an example worker built and run
> against a broker in CI. Built against the published `AceMq.Amqp` 0.7.2.

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

```csharp
builder.Services
    .AddAceMq(builder.Configuration.GetSection("acemq"))
    .AddConsumer<Order, OrderHandler>("orders.new");

sealed class OrderHandler(OrderBook book) : IMessageHandler<Order>
{
    public async Task<Ack> HandleAsync(IMessage<Order> message, CancellationToken ct)
    {
        await book.RecordAsync(message.Payload, ct);
        return Ack.Accept();
    }
}
```

```csharp
sealed class Orders(AceMqConnection mq)
{
    public Task Place(Order order) =>
        mq.Publisher<Order>("orders", "order.created").SendAsync(order);
}
```

That is the whole surface for the common case: a connection to inject, and an interface for
the other direction.

## Installing

Two packages:

| Package | What it is |
|---|---|
| `AceMq.Amqp.Hosting` | What an application adds: `AddAceMq`, the consumer host, the health check, and — because transports here are registered by hand rather than scanned for — the RabbitMQ transport |
| `AceMq.Amqp.Hosting.OpenTelemetry` | `AddAceMqInstrumentation()` on the tracer and meter builders. Separate, so an application without OpenTelemetry does not carry it |

### Versions

| | version |
|---|---|
| `AceMq.Amqp.Hosting`, `AceMq.Amqp.Hosting.OpenTelemetry` | **0.1.0** |
| `AceMq.Amqp`, `AceMq.Amqp.RabbitMq` this is built and tested against | **0.7.2** |
| `Microsoft.Extensions.*` | 8.0.0, and see [requirements](#requirements) for why not the newest |

The two AceMQ numbers are on separate lines on purpose, and one is not behind the other —
see [its own version line](#its-own-version-line). The library comes down as a dependency,
so an application names only the hosting package. `<AceMqVersion>` in
`Directory.Build.props` is where the library version this repository pins actually lives;
a CI job fails the build when it falls behind the newest release.

**Pin an exact version while this package is `0.x`.** The public surface may change in any
release, which is what semver means by leaving `0.y.z` outside its guarantees.

```bash
dotnet add package AceMq.Amqp.Hosting --version 0.1.0
dotnet add package AceMq.Amqp.Hosting.OpenTelemetry --version 0.1.0   # if you use OpenTelemetry
```

```xml
<PackageReference Include="AceMq.Amqp.Hosting" Version="0.1.0" />
<PackageReference Include="AceMq.Amqp.Hosting.OpenTelemetry" Version="0.1.0" />
```

The nuspec asks for `AceMq.Amqp` 0.7.2 **or newer**, which is how NuGet reads a bare
version, so an application already on a later library keeps it.

### The feed

AceMQ is not on nuget.org before 1.0:

```xml
<packageSources>
  <clear />
  <add key="nuget.org" value="https://api.nuget.org/v3/index.json" protocolVersion="3" />
  <add key="acemq" value="https://acemq.org/nuget/index.json" protocolVersion="3" />
</packageSources>
<packageSourceMapping>
  <packageSource key="acemq"><package pattern="AceMq.*" /></packageSource>
  <packageSource key="nuget.org"><package pattern="*" /></packageSource>
</packageSourceMapping>
```

The source mapping is not decoration: without it an unauthenticated feed in the list can
answer for any package id.

## Requirements

`netstandard2.0` and `net8.0`, the same two targets `acemq-dotnet-amqp` ships — so .NET
Framework 4.6.2+ as well as modern .NET. That survived only because the
Microsoft.Extensions packages this needs still carry a netstandard2.0 asset on the **8.0**
line, which is why the dependency is pinned there rather than to the newest. NuGet resolves
to the highest version in the graph, so an application on a newer line gets the newer
assemblies anyway and the pin costs it nothing.

CI checks the built assembly and the packed `lib/` folders rather than the project file,
because a `<TargetFrameworks>` line can say `netstandard2.0` and still fail to produce one.

Docker for the integration tests.

## What it configures

- **A connection** — URL, credentials, virtual host, client name, timeouts, publisher
  confirms, `maxOutstandingPublishes`, codec and TLS, from the `acemq` section. `AceMqConnection`
  is injectable; `IAceMqConnectionProvider` is the async-correct way to get the same one.
- **A topology** — exchanges, queues and bindings, declared before any consumer starts.
  Nothing at all unless something is declared.
- **Consumers** — one per `AddConsumer`, each a `ConsumerGroup`, started after the host is
  built and drained when it stops. Prefetch, concurrency, a retry ladder and an
  [idempotency store](docs/retries.md#handling-a-message-once) per consumer.
- **A health check** — registered as `acemq`, tagged `ready`.
- **Telemetry** — the library's `ActivitySource` and `Meter`, reachable in one line from an
  `AddOpenTelemetry()` application.

## Documentation

Eighteen pages at **<https://acemq.org/acemq-dotnet-amqp-hosting/>**. They read as markdown in
[docs/](docs/) too, and render with `.github/scripts/build-docs-site.sh`.

| | |
|---|---|
| **Start here** | [docs/index.md](docs/index.md) · [Getting started](docs/getting-started.md) |
| **Reference** | [Configuration](docs/configuration.md) — every setting under `acemq` |
| **Usage** | [Handlers and consumers](docs/handlers.md) · [Topology](docs/topology.md) · [Publishing](docs/publishing.md) · [Serialization and codecs](docs/serialization.md) · [Testing](docs/testing.md) |
| **Patterns** | [Patterns from a host](docs/patterns.md) — the map · [Retries and duplicates](docs/retries.md) · [Request and reply](docs/request-reply.md) · [Transactional outbox](docs/outbox.md) · [Streams](docs/streams.md) |
| **Operations** | [Startup and shutdown](docs/lifecycle.md) · [Health checks](docs/health.md) · [Metrics and tracing](docs/observability.md) · [Security](docs/security.md) |
| **Support** | [Enterprise support](https://acemq.com) |

**[Patterns from a host](docs/patterns.md)** is the page to read second. The library has an
outbox, sagas, streams, request-reply, scheduling, pipelines, claim checks and replay, and
most of them have no setting under `acemq` — because an outbox needs a database and a saga is
three lambdas. That page says where each one lives, which of them are configuration and which
are a service you register, and the one ordering rule that makes hand-wiring correct.

## Three decisions worth knowing about

### Handlers are an interface, not an attribute

The Spring Boot starter turns an annotated method into a consumer. That is right for
Spring, whose whole model is a post-processor finding annotations on beans the container
already owns. .NET has no ambient scanning phase, and adding one would mean either
reflecting over every loaded assembly at startup or asking the application to list its
assemblies.

So a handler here is a type:

```csharp
public interface IMessageHandler<T>
{
    Task<Ack> HandleAsync(IMessage<T> message, CancellationToken cancellationToken);
}
```

Constructor injection works with no special case. A fresh `IServiceScope` is created per
message, so a `DbContext` behaves exactly as it does per-request and two concurrent messages
never share one. `AddConsumer<Order, OrderHandler>` does not compile unless `OrderHandler`
really handles `Order`. Decorators and interceptors apply, because it is a normal
registration. Testing one is calling a method.

The cost is a class where an attribute on a method would have done.

### A blocked broker is healthy, and a draining instance is not

RabbitMQ blocks a connection when it is low on memory or disk. An application that fails its
own health check for that is one an orchestrator restarts — into the same blocked broker,
having thrown away whatever it was holding. So a blocked connection is reported **healthy,
with the reason in the description and the data**, as the Spring starter reports it.

`Degraded` was the obvious alternative. It maps to 200 in the framework's defaults, but
`ResultStatusCodes` is routinely changed to map it to 503, and a choice whose safety depends
on a setting in somebody else's file is not a choice.

Draining is the opposite call for the opposite reason: from the moment shutdown begins the
check reports **unhealthy**, so the instance leaves the rotation before it starts refusing
work. Which is also why it should not be wired to a liveness probe.

### The token a handler gets is not the host's stopping token

A handler given the host's stopping token sees it cancelled the moment shutdown begins, so
it aborts — and the drain that was meant to let it finish is what killed it.

The token here stays uncancelled through the whole of a graceful drain. It is cancelled only
once the drain has already overrun, which is the signal to give the delivery back to the
broker rather than finish it. Cancellation is the last thing shutdown does, never the first.

## What a drain does, and does not

`StopAsync` stops handing messages to handlers, waits for the ones already running, and only
then cancels.

| at the moment of shutdown | what happens |
|---|---|
| a handler is running | it runs to completion, and its decision is carried out |
| a delivery has arrived but no handler has it | it is held unacknowledged, and the broker redelivers it |
| a publish is waiting for its confirm | nothing waits for it; it is cut off |
| a retry is waiting out a backoff in this process | the drain waits with it, for the whole delay |

Three honest limits follow from that table, and they are each expanded on the
[lifecycle page](docs/lifecycle.md):

- **A drain is bounded by `concurrency`, not `prefetch`.** That makes it fast and makes the
  redelivery burst after it larger — up to `prefetch` messages per consumer come back. This
  differs from the Go library, which handles them instead. "Drained cleanly" means *every
  handler finished*, not *every message the broker had sent was handled*.
- **An in-process retry backoff is waited out in full.** A five-minute rung waited for in
  the process is a five-minute drain, which is to say a failed one. Lower
  `acemq:listener:retry:brokerWaitThreshold` and the waits move to the broker, where a
  restart does not have to survive them.
- **The host's `ShutdownTimeout` wins.** A drain configured for sixty seconds under a host
  that waits thirty gets thirty. The consumer host warns at startup when they are set that
  way round.

## What it does not do

- **It is not a second messaging library.** Everything it configures is `AceMq.Amqp`. An
  application that outgrows what is configurable here drops to the library without leaving
  anything behind — the connection it injects is the library's own, with the library's whole
  surface on it: outbox, sagas, streams, ordered queues, request-reply, interceptors.
  [Patterns from a host](docs/patterns.md) shows each of those wired into a host, and says
  plainly which ones have no setting and never will.
- **No `appsettings.json` IntelliSense.** The Spring starter ships configuration metadata
  and properties complete as you type them. There is no equivalent here yet. XML
  documentation gives completion on the `AddAceMq(o => ...)` callback; the JSON file gets
  nothing. This is a real gap and it is listed as one.
- **No queue name inferred from a handler.** A typo in a queue name should fail loudly, not
  quietly create the queue it was a typo for.
- **No ASP.NET Core-specific anything.** It registers a health check and stops there; where
  that check is mapped, and what it is exposed to, is the application's business.
- **No retries by default.** A retry that is on by default is a retry nobody chose.

## Its own version line

0.1.0, while the library is at 0.7.2. This package tracks two release trains — AceMQ's and
Microsoft.Extensions' — and a change in either can force a release here. A shared version
number could only say that one of the two had moved, which tells a consumer nothing about
which. The Spring Boot starter is a separate repository on its own line for the same reason.

`AceMq.Amqp` is therefore a **package** dependency, not a project reference. This repository
exists partly to prove the published package is usable from an application, and a project
reference would prove only that its source is. CI checks the produced nuspec for it.

[RELEASING.md](RELEASING.md) has the rest: where the packages are published, what the
release-line guard refuses, and how to check that a release actually landed.

## Licence

Apache-2.0. See [LICENSE](LICENSE), [NOTICE](NOTICE), and
[docs/licence.md](docs/licence.md) for what the warranty disclaimer means in practice.
