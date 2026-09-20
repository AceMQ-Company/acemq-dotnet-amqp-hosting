# Metrics and tracing

The library instruments itself with the runtime's own `ActivitySource` and `Meter`. There
is nothing to start, nothing to inject and no adapter: a collector that subscribes to the
two names sees everything.

## OpenTelemetry

```bash
dotnet add package AceMq.Amqp.Hosting.OpenTelemetry
```

```csharp
builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddAceMqInstrumentation().AddOtlpExporter())
    .WithMetrics(m => m.AddAceMqInstrumentation().AddOtlpExporter());
```

One line each. It is a second package rather than something `AddAceMq` does, and the reason
is a dependency: `AddAceMq` cannot subscribe an OpenTelemetry pipeline it cannot see, and
making `AceMq.Amqp.Hosting` depend on OpenTelemetry would put it in the dependency graph of
every application that uses AceMQ and not OpenTelemetry. So the wiring lives in the package
an application adds precisely because it has both — which is how every other .NET
instrumentation library is arranged, and means the line looks like the ones around it.

It depends on `OpenTelemetry.Api` only, not the SDK, so your application keeps its own
choice of SDK version.

## Without OpenTelemetry

The two names are constants:

```csharp
using AceMq.Amqp.Hosting;

AceMqTelemetryNames.ActivitySource   // "AceMq.Amqp"
AceMqTelemetryNames.Meter            // "AceMq.Amqp"
```

Enough for a hand-rolled `MeterListener`, an `ActivityListener` in a test, or a
`dotnet-counters` session:

```bash
dotnet-counters monitor --process-id <pid> --counters AceMq.Amqp
```

## What is counted

The instruments and their names are the library's, and they match the Java and Go libraries
so one dashboard serves a polyglot fleet. The full list is on the library's
[observability page](https://acemq.org/acemq-dotnet-amqp/observability.html); the ones that
matter most for a worker:

| | |
|---|---|
| `acemq.consume.total` | messages handled, tagged by queue and outcome |
| `acemq.consume.duration` | how long handlers took |
| `acemq.consume.in.flight` | messages currently inside a handler — the number a drain waits down to zero |
| `acemq.consume.attempts` | which attempt a message was handled on |
| `acemq.messages.retried.total` | retries, tagged by rung |
| `acemq.messages.dead.lettered.total` | |
| `acemq.retry.rung.missing` | a retry that had to wait in the process because its rung queue was absent |
| `acemq.publish.total` | publishes, tagged by exchange and outcome |

`acemq.retry.rung.missing` is worth an alert on its own merits, and is also the early warning
that the next deployment's drain is going to be slow — see
[startup and shutdown](lifecycle.md).

## Logs

Everything this package does is logged through `ILogger`, on the category of the class that
did it:

| category | at | what |
|---|---|---|
| `AceMq.Amqp.Hosting.AceMqConnectionProvider` | Information | connecting, and connected with the transport's capabilities |
| `AceMq.Amqp.Hosting.AceMqConnectionHost` | Information | the topology plan, when it changed anything |
| | Warning | drift between the declared topology and the broker's |
| `AceMq.Amqp.Hosting.AceMqConsumerHost` | Information | each consumer starting, with its queue, concurrency and prefetch |
| | Information | the drain beginning, how long it took, and how many deliveries it handed back unhandled |
| | Warning | a drain that overran, a drain the host stopped waiting for, and the shutdown-timeout misconfiguration that causes most of them |

Those last two are logged as the separate events they are. A drain that ran out of its own
`listener:shutdownTimeout` names that budget; a drain the host cancelled at
`HostOptions.ShutdownTimeout` says so and names both times, which is the difference between
"the handlers are too slow" and "the deadline is set wrong". See
[startup and shutdown](lifecycle.md#the-two-deadlines).

The connection URL is logged with its password replaced. That is the library's `ToString`,
not something this package does, and it is worth knowing it is there rather than
discovering it is not.
