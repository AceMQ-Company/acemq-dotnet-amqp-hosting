# Streams

A stream is a log that is read rather than emptied. Reading a message does not remove it, so
several readers can each be at a different place in the same stream, and a reader can start
from the beginning, from a timestamp, or from an offset it recorded last time.

This page is half configuration and half hand-wiring, and the split is worth stating first,
because it is the thing this page exists to say:

- **Declaring a stream is configuration.** `acemq:topology` does it, like any other queue.
- **Reading a stream is not.** `AddConsumer` cannot read a stream, because a stream reader
  takes an offset and an `AddConsumer` has nowhere to put one. Reading is a hosted service
  holding a `StreamReader<T>`.

## Declaring one

`type: "Stream"` in the topology, with the retention arguments the broker understands:

```json
{
  "acemq": {
    "topology": {
      "exchanges": [{ "name": "events", "type": "topic" }],
      "queues": [
        {
          "name": "events.log",
          "type": "Stream",
          "arguments": {
            "x-max-age": "7D",
            "x-max-length-bytes": "5000000000"
          }
        }
      ],
      "bindings": [
        { "queue": "events.log", "exchange": "events", "routingKey": "event.#" }
      ]
    }
  }
}
```

| Argument | |
|---|---|
| `x-max-age` | how long to keep. RabbitMQ's own units: `7D`, `12h`, `30m`, `1Y` |
| `x-max-length-bytes` | a size cap, whichever is reached first |
| `x-stream-max-segment-size-bytes` | segment size; leave it alone unless you have measured a reason |

`x-max-age` is a **broker** duration and not a `TimeSpan`, so it is `"7D"` and not
`"7.00:00:00"`. That is an exception to the rule that durations in this file are `TimeSpan`s,
and it is because these strings are passed to the broker as declared rather than being
interpreted here. The [topology page](topology.md#queues) explains the numeric coercion that
makes `"5000000000"` arrive as a number rather than a string.

`deadLetter: true` makes no sense on a stream and should not be set. A stream has nowhere to
dead-letter *to* in the sense a queue does — nothing is being rejected, because nothing is
being consumed off the front.

**A stream must be declared before anything reads it**, which is what putting it in
`acemq:topology` buys: `AceMqConnectionHost` applies the topology before any hosted service
of yours starts. See [startup and shutdown](lifecycle.md#the-order).

### Or in code, for the arguments configuration cannot express

`DeclareStreamAsync` is typed where the arguments are strings:

```csharp
await mq.DeclareStreamAsync(
    "events.log",
    maxAge: TimeSpan.FromDays(7),
    maxLengthBytes: 5_000_000_000);
```

There is a four-argument overload taking `segmentBytes` as well. Both are `null`-accepting,
so a stream with no retention at all is `DeclareStreamAsync(name, null, null)` — and is
almost always a mistake, because a stream with no cap grows until the disk is full.

If you declare it this way, do it from a hosted service registered after `AddAceMq` and
before anything reads it, exactly as [topology](topology.md#declaring-more-than-configuration-can-express)
describes.

## Reading one

`connection.Stream<T>(queue)` returns a `StreamReader<T>`, which is a builder: pick an
offset, then `ConsumeAsync`.

```csharp
builder.Services
    .AddAceMq(builder.Configuration.GetSection("acemq"));

// After AddAceMq, so the connection is open and the stream is declared before this runs.
builder.Services.AddHostedService<EventProjection>();

public sealed class EventProjection : IHostedService
{
    private readonly IAceMqConnectionProvider _connections;
    private readonly IServiceProvider _services;
    private readonly ILogger<EventProjection> _log;
    private IStreamConsumer? _reading;

    public EventProjection(
        IAceMqConnectionProvider connections,
        IServiceProvider services,
        ILogger<EventProjection> log)
    {
        _connections = connections;
        _services = services;
        _log = log;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var mq = await _connections.GetAsync(cancellationToken);

        _reading = await mq.Stream<Event>("events.log")
            .FromFirst()
            .Prefetch(200)
            .ConsumeAsync(Handle);

        _log.LogInformation("reading events.log from the beginning");
    }

    private async Task Handle(IMessage<Event> message)
    {
        // A scope per message, because that is what AddConsumer would have given a
        // handler and a projection wants a DbContext just as much.
        using var scope = _services.CreateScope();
        var projections = scope.ServiceProvider.GetRequiredService<Projections>();
        await projections.ApplyAsync(message.Payload);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _reading?.Dispose();
        _log.LogInformation(
            "stopped at offset {Offset} after {Handled} message(s)",
            _reading?.LastHandledOffset,
            _reading?.Handled);
        return Task.CompletedTask;
    }
}
```

Three things in there are the point of the page.

**The handler returns `Task`, not `Task<Ack>`.** A stream reader decides the outcome for you
— acknowledging a stream delivery does not remove it, which is what makes a stream a stream,
so there is no interesting choice to offer. `Ack.Accept()`, `Ack.DeadLetter()` and the rest
of the vocabulary on [the handlers page](handlers.md#what-a-handler-returns) are not yours to
return here. What a failure does instead is [below](#failure-and-skipfailures).

**Nothing under `acemq:listener` touches a stream reader.** Prefetch is `Prefetch(n)` on the
reader; the retry ladder, `requeueOnFailure` and `shutdownTimeout` are all settings for the
consumers `AddConsumer` started and none of them reach here. There is no concurrency setting,
because a stream is read in order by one reader — which is the property a projection is
relying on.

**The scope is yours to create.** `AddConsumer` creates an `IServiceScope` per message;
nothing does that here, so a reader that touches scoped services creates its own. Forgetting
this is how a `DbContext` ends up shared across every message in a stream, which fails at the
second one.

## Where to start reading

| | |
|---|---|
| `FromFirst()` | the beginning of the stream, however much that is |
| `FromLast()` | the **last message already in the stream**, so one message is re-handled |
| `FromNext()` | only what arrives from now on |
| `FromOffset(long)` | an exact offset, which is what a stored position is |
| `FromTime(DateTimeOffset)` | the first message at or after an instant |
| `FromLast(TimeSpan)` | everything from an age ago — `FromLast(TimeSpan.FromHours(1))` |

`FromLast()` and `FromLast(TimeSpan)` are different methods that read almost the same, and
the difference catches people: no argument means *the last message*, an argument means
*everything since*.

These are conveniences over `StreamOffset`, which has the same set as static factories —
`First()`, `Last()`, `Next()`, `At(offset)`, `From(timestamp)`, `LastFor(age)` — for when the
offset is a value to pass around rather than a call to make. `From(StreamOffset)` takes one.

### Remembering where you were

**The library does not store offsets and this package does not either.** A restart with
`FromFirst()` re-reads the whole stream; a restart with `FromNext()` misses everything the
process was down for. Neither is what a projection wants, and the answer is to store the
offset yourself — in the same database the projection writes to, so the position and the
data it produced move together:

```csharp
public async Task StartAsync(CancellationToken cancellationToken)
{
    var mq = await _connections.GetAsync(cancellationToken);

    using var scope = _services.CreateScope();
    var positions = scope.ServiceProvider.GetRequiredService<Positions>();
    var last = await positions.ReadAsync("events.log", cancellationToken);

    var reader = mq.Stream<Event>("events.log");
    _reading = await (last == null ? reader.FromFirst() : reader.FromOffset(last.Value + 1))
        .ConsumeAsync(Handle);
}
```

`last.Value + 1` and not `last.Value`: the stored offset is the last one *handled*, and
`FromOffset` is inclusive, so passing it back unchanged re-handles that message. Whether
that matters depends on whether the projection is idempotent — and a projection that is
idempotent is one that can afford `FromOffset(last.Value)` and be certain instead.

`IStreamConsumer.LastHandledOffset` is the number to store. Store it from the handler
rather than only at shutdown: a process killed rather than stopped never reaches `StopAsync`.

## Failure, and `SkipFailures`

A handler that throws is **retried after five seconds**, indefinitely, and each attempt
increments the reader's `Failed` counter. Five seconds is the reader's own fixed interval;
`acemq:listener:retry` does not apply and there is no ladder.

That is the safe default for a projection, because a message that cannot be applied must not
be stepped over: message 400 applied before 399 produces a wrong answer that nobody notices.
Retrying for ever is the conservative failure — the reader makes no progress past the bad
message, `Failed` climbs, and somebody has to look. Which is the point.

It is also a trap if you were expecting it to give up. **A permanently failing message stops
a projection advancing, quietly, for as long as nobody is watching `Failed`.** Put it on a
dashboard, or take the other choice.

`SkipFailures()` is the other choice: the failure is counted in `Skipped`, the message is
dead-lettered, and reading moves on.

```csharp
_reading = await mq.Stream<Event>("events.log")
    .FromLast(TimeSpan.FromHours(1))
    .SkipFailures()
    .ConsumeAsync(Handle);
```

That is right for an audit feed or a metrics tap, where one unparseable message should not
hold up the other thousand, and wrong for anything that accumulates state. Decide which you
have; the default assumes the second.

**A skipped message is not lost.** A stream reader declares `{stream}.dlq` and
`{stream}.parked` beside the stream, the same pair every consumer gets, and a skipped message
goes to `events.log.dlq` where it can be inspected and
[replayed](patterns.md#replay). That is worth knowing before turning `SkipFailures()` on: two
queues appear on the broker that were not in your topology, and they are meant to.

Either way the reader exposes what happened, and these four belong on a dashboard:
`Handled`, `Failed`, `Skipped`, `LastHandledOffset`. `IsActive` is the fifth, for the case
where the reader has gone away entirely:

```csharp
sealed class ProjectionHealth : IHealthContributor
{
    private readonly EventProjection _projection;

    public ProjectionHealth(EventProjection projection) => _projection = projection;

    public string Name => "events-projection";

    public HealthReport Report() => new HealthReport(
        Name,
        _projection.IsReading
            ? AceMq.Amqp.HealthStatus.Up
            : AceMq.Amqp.HealthStatus.Down,
        new Dictionary<string, string>
        {
            ["offset"] = _projection.LastOffset?.ToString() ?? "none",
        });
}
```

Registered with `connection.RegisterHealth(...)`, that appears in this package's health check
as `check.events-projection` and takes the instance out of the readiness rotation when the
reader has stopped. See [health checks](health.md#contributors) — and note that
`AceMq.Amqp.HealthStatus` is not
`Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus`; inside a namespace under
`AceMq.Amqp` you will need an alias.

## Several readers, one stream

The whole point of a stream. Each reader is a separate `ConsumeAsync` with its own offset,
and they do not interfere: a projection reading from the beginning, an audit tap reading only
what is new, and a debugging reader started at a timestamp can all be on one stream at once.

Registered as one hosted service per reader, or one hosted service holding several — the
second is usually tidier, because they share a connection and a shutdown.

What they cannot do is share the work. Several readers over one stream each see **every**
message; that is not a consumer group. If the goal is to spread load rather than to fan out,
the tool is an ordinary queue with `concurrency` above one, or a partitioned
[ordered queue](patterns.md#ordered-queues).

## Publishing into one

Ordinary. A stream bound to an exchange receives what the exchange routes, so the publisher
is the same publisher as for any other queue and nothing about it knows the destination is a
stream:

```csharp
await mq.Publisher<Event>("events", "event.created").SendAsync(theEvent);
```

Confirms work as they do everywhere. See [publishing](publishing.md).

## Shutdown

A stream reader is **not part of the drain**, and this is the last thing worth knowing.

`AceMqConsumerHost.StopAsync` drains the consumers `AddConsumer` registered.
`connection.DrainConsumersAsync` — which is what it calls — pauses consuming and waits for
handlers, and a stream reader's handler is inside that count; but the reader itself is
disposed by *your* `StopAsync`, which runs **before** the consumer host's, because hosted
services stop in reverse registration order. So:

- a stream handler running when shutdown begins is cut off by the `Dispose()` in your
  `StopAsync`, not waited for;
- if that matters — and for a projection mid-write it does — do the waiting yourself in
  `StopAsync`, before disposing.

There is no equivalent of the [drain](lifecycle.md#what-a-drain-finishes-precisely) for
streams, and there is nothing to redeliver either: nothing was acknowledged, so a restart
picks up from the offset you stored. Which is the argument for storing it per message rather
than at shutdown, one more time.

## The broker has to support them

`Capability.Streams` reports whether the transport does:

```csharp
if (!mq.Supports(Capability.Streams))
{
    throw new InvalidOperationException("this broker has no streams");
}
```

RabbitMQ has had stream queues since 3.9, and the library's integration suite covers them on
both 4.x and 3.13.

The library's in-memory transport — `memory://`, which is what the
[testing page](testing.md) recommends for wiring tests — **does not report
`Capability.Streams`**. So a stream reader is one of the things that cannot be tested there,
and the check above is the right thing for a hosted service to do at startup rather than
discovering it at the first read. Test a stream reader against a real broker.
