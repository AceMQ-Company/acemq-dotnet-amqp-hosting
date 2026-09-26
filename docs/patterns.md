# Patterns from a host

This package configures three things: a connection, a topology, and consumers. The library
it configures does considerably more than that — an outbox, sagas, streams, request-reply,
scheduling, pipelines, claim checks, replay, interceptors — and **most of those have no
setting under `acemq` and never will**.

That is not an oversight, and this page is the map. A pattern reachable from
`appsettings.json` is a pattern whose whole configuration is scalars. An outbox needs a
database connection; a claim check needs a blob store; a saga is three lambdas. Naming any
of those in a JSON file would mean this package constructing database connections and
resolving delegates by reflection, which is the application's business and not a message
library's.

So the answer for those is **hand-wiring**, and the honest version of hand-wiring in a
.NET host is short: they are services. The rest of this page is where each pattern lives,
and the two shapes that cover all of them.

## Where each pattern lives

| Pattern | How it is reached |
|---|---|
| Publishing | the injected `AceMqConnection` — [publishing](publishing.md) |
| Consuming | `AddConsumer` — [handlers](handlers.md) |
| Retries, dead letters, parking | `acemq:listener:retry` and per-consumer — [retries](retries.md) |
| Handling a message once | per-consumer `r.Idempotency` — [retries](retries.md#handling-a-message-once) |
| Exchanges, queues, bindings, drift | `acemq:topology` — [topology](topology.md) |
| Codec, by name | `acemq:format` — [serialization](serialization.md) |
| A codec of your own, encryption, claim check, Avro | `CodecRegistry.Register`, **before the host starts** — [serialization](serialization.md) |
| TLS, credentials, development certificates | `acemq:tls` and `acemq:url` — [security](security.md) |
| Health, and contributors of your own | registered for you; contributors by hand — [health](health.md) |
| Metrics and tracing | `AddAceMqInstrumentation()` — [observability](observability.md) |
| Back pressure, and a blocked broker | `acemq:maxOutstandingPublishes`, and the health check — [below](#back-pressure-and-a-blocked-broker) |
| **Streams** | declared in `acemq:topology`, read by hand — [streams](streams.md) |
| **Request and reply** | by hand — [request and reply](request-reply.md) |
| **Transactional outbox** | by hand — [outbox](outbox.md) |
| **Scheduling** | by hand — [below](#scheduling) |
| **Interceptors** | by hand — [below](#interceptors) |
| **Replay** | by hand — [below](#replay) |
| **Sagas** | by hand, and nothing to do with the connection — [below](#sagas) |
| **Pipelines and routing slips** | by hand — [below](#pipelines) |
| **Ordered queues** | by hand — [below](#ordered-queues) |
| **Pulling one message** | by hand — [below](#pulling-one-message) |

Everything in the second half of that table takes the injected connection and nothing from
this package. That is the point of the arrangement rather than a limitation of it: an
application that outgrows what is configurable here has nothing to unpick, because the
connection it already has is the library's own.

## The two shapes

Hand-wiring a library pattern into a host is a choice between two shapes, and **choosing
the wrong one is the only way to get this wrong**. The rule is short:

- Something a **handler calls** is a **lazily-initialised singleton**.
- Something that **drives work of its own** is an **`IHostedService` registered after
  `AddAceMq`**.

The reason is shutdown ordering, and it is worth spelling out, because it is not obvious
and it does not fail loudly.

`AddAceMq` registers two hosted services, `AceMqConnectionHost` then `AceMqConsumerHost`.
The generic host starts hosted services in registration order and **stops them in
reverse**. So a hosted service of yours registered after `AddAceMq`:

- **starts last** — after the connection is open and the topology is applied, which is what
  you want;
- **stops first** — *before* `AceMqConsumerHost` drains.

That second line is the trap. A `Scheduler` or a `Requester` owned by a hosted service is
already stopped while handlers are still finishing their messages, and a handler that
reaches for it during a drain finds it disposed. Nothing about that is reported as an
ordering problem; it arrives as an `ObjectDisposedException` on the last few messages of
every deployment.

Container **singletons**, by contrast, are disposed after every hosted service has stopped
— which is after the drain. `IAceMqConnectionProvider` relies on exactly that for the
connection itself, and it is the right place for anything a handler uses.

### Shape one: a singleton a handler calls

The awkward part is that connecting is asynchronous and constructing a service is not, so
the thing is built on first use rather than in the constructor:

```csharp
builder.Services.AddSingleton<Reminders>();

public sealed class Reminders : IDisposable
{
    private readonly IAceMqConnectionProvider _connections;
    private readonly SemaphoreSlim _gate = new SemaphoreSlim(1, 1);
    private Scheduler? _scheduler;

    public Reminders(IAceMqConnectionProvider connections) => _connections = connections;

    public async Task RemindAsync(TimeSpan delay, Reminder reminder, CancellationToken ct)
    {
        var scheduler = await SchedulerAsync(ct);
        await scheduler.InAsync(delay, "reminders", "reminder.due", reminder);
    }

    private async Task<Scheduler> SchedulerAsync(CancellationToken ct)
    {
        var existing = _scheduler;
        if (existing != null) return existing;

        await _gate.WaitAsync(ct);
        try
        {
            // Scheduler.OnAsync declares a topology of its own, so it is built once and
            // not once per call.
            return _scheduler ??= await Scheduler.OnAsync(await _connections.GetAsync(ct));
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose()
    {
        _scheduler?.Dispose();
        _gate.Dispose();
    }
}
```

That is more ceremony than the library's own examples need, and all of it is the gap
between an async factory and a constructor. `IAceMqConnectionProvider` is written the same
way for the same reason, and is worth reading if this shape is new.

Register it **before or after `AddAceMq`, it does not matter** — a singleton is not
ordered, only hosted services are.

### Shape two: a hosted service that drives work

```csharp
builder.Services
    .AddAceMq(builder.Configuration.GetSection("acemq"))
    .AddConsumer<Order, OrderHandler>("orders.new");

// After AddAceMq: starts once the connection is open, stops before the consumers drain.
builder.Services.AddHostedService<Projections>();

public sealed class Projections : IHostedService
{
    private readonly IAceMqConnectionProvider _connections;
    private IDisposable? _running;

    public Projections(IAceMqConnectionProvider connections) => _connections = connections;

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var mq = await _connections.GetAsync(cancellationToken);
        _running = await mq.Stream<Event>("events").FromLast(TimeSpan.FromHours(1))
            .ConsumeAsync(Handle);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _running?.Dispose();
        return Task.CompletedTask;
    }

    private Task Handle(IMessage<Event> message) => Task.CompletedTask;
}
```

`IHostedService` rather than `BackgroundService` whenever the work is a subscription rather
than a loop — a `BackgroundService` whose `ExecuteAsync` only sleeps until cancellation is
a pattern that looks like work and is not. `AceMqConsumerHost` is an `IHostedService` for
that reason. A `BackgroundService` is right when there really is a loop: a poller, a timer,
a producer.

## Interceptors

An interceptor is cross-cutting work on the publish or consume path — a tenant stamped on
every message, every handled message timed — without either concern appearing in a handler.

`connection.Intercept(...)` is the library's registration, and there is **no
`IEnumerable<IPublishInterceptor>` convention in this package**: nothing collects
interceptors out of the container. Interceptors are ordered, by their own `Order` property,
and a set assembled from whatever happens to be registered is a set whose order depends on
registration order — so the wiring is explicit:

```csharp
builder.Services.AddSingleton<TenantStamp>();
builder.Services.AddSingleton<TimeEveryHandler>();
builder.Services.AddHostedService<Interceptors>();

// Before AceMqConsumerHost would be better still, but the connection is not open until
// AceMqConnectionHost has run. Registered after AddAceMq, this starts third — after the
// connection and after the consumers. See below for why that is nonetheless safe.
sealed class Interceptors : IHostedService
{
    private readonly IAceMqConnectionProvider _connections;
    private readonly TenantStamp _stamp;
    private readonly TimeEveryHandler _timing;

    public Interceptors(
        IAceMqConnectionProvider connections, TenantStamp stamp, TimeEveryHandler timing)
    {
        _connections = connections;
        _stamp = stamp;
        _timing = timing;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var mq = await _connections.GetAsync(ct);
        mq.Intercept(_stamp).Intercept(_timing);
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}

sealed class TenantStamp : PublishInterceptor
{
    private readonly ITenant _tenant;

    public TenantStamp(ITenant tenant) => _tenant = tenant;

    public override PublishContext BeforePublish(PublishContext context)
    {
        // .NET's Envelope has no ToBuilder, so the fields are carried across by hand.
        // Miss one and the message loses it, which is why this is written out rather
        // than hidden in a helper.
        var stamped = Envelope.Of(context.Envelope.Type)
            .Id(context.Envelope.Id)
            .Version(context.Envelope.Version)
            .CorrelationId(context.Envelope.CorrelationId)
            .CausationId(context.Envelope.CausationId)
            .Attempt(context.Envelope.Attempt)
            .FirstSeen(context.Envelope.FirstSeen)
            .Origin(context.Envelope.Origin)
            // x-acemq- is the library's own namespace and is dropped on consume, so an
            // application header needs a prefix of its own.
            .Header("x-tenant", _tenant.Current);

        foreach (var header in context.Envelope.Headers)
        {
            stamped.Header(header.Key, header.Value);
        }

        return context.WithEnvelope(stamped.Build());
    }

    public override int Order => 10;
}
```

The copying is not decoration. An interceptor may replace the envelope and nothing else —
one that could rewrite the payload or the destination could send a message somewhere the
caller never asked for — and replacing means replacing, so anything not carried across is
lost.

Note also that an interceptor is an ordinary service: `TenantStamp` takes `ITenant` from the
container, which is the whole reason for registering it there rather than newing it up.

**There is a race here and it is worth naming.** Consumers start before that
`StartAsync` runs, so a message that arrives in the first milliseconds is handled without
the consume interceptors attached. For a metric that is invisible; for anything load-bearing
it is not. Two ways round it, both honest:

- Register the interceptor hosted service **before `AddAceMq`** and have it resolve the
  connection with `GetAsync()` — which opens the connection early, in `StartAsync`, before
  `AceMqConnectionHost` would have. That closes the race at the cost of connecting from a
  service whose job is not connecting.
- Or do it in `Program.cs` between `Build()` and `RunAsync()`, which is clearer and blocks:

  ```csharp
  var host = builder.Build();
  var mq = await host.Services.GetRequiredService<IAceMqConnectionProvider>().GetAsync();
  mq.Intercept(new TenantStamp());
  await host.RunAsync();
  ```

`PublishInterceptor` and `ConsumeInterceptor` are abstract classes with no-op virtuals, so
override only the hook you need. The interfaces are `IPublishInterceptor` and
`IConsumeInterceptor` if you would rather implement all four members.

A publish interceptor may **replace** the envelope, through `context.WithEnvelope(...)`; a
consume interceptor sees `BeforeHandle`, `AfterHandle` and `OnError` and changes nothing.
That asymmetry is the library's and is deliberate: a consume interceptor that could rewrite
a message would be a handler with no name.

## Replay

Dead-lettering is half a story. The other half is that the messages are kept so they can
have another run once whatever broke is fixed.

Replay is a one-off operation, not a subscription, so it belongs behind something a person
triggers — a minimal API endpoint is usually right:

```csharp
app.MapPost("/admin/replay/orders", async (IAceMqConnectionProvider connections, CancellationToken ct) =>
{
    var mq = await connections.GetAsync(ct);
    var replay = mq.Replay("orders.new.dlq").Into("orders.new");

    var pending = await replay.PendingAsync();
    var moved = await replay.ReplayAsync(max: 500);

    return Results.Ok(new { pending, moved });
});
```

`Replay(queue)` reads the queue named; `.Into(queue)` says where they go, and without it
they go back where they came from. `.KeepingAttempts()` leaves the attempt counter alone,
which means a message that had exhausted its ladder exhausts it again immediately — that is
occasionally what you want and usually not, so the default restarts the count.
`ReplayAsync(max, filter)` takes a predicate over the raw delivery, for putting back only
the ones that failed for the reason that has been fixed.

**Bound it.** `ReplayAllAsync()` on a dead-letter queue holding a week of failures is a
week of failures arriving at once, through a consumer sized for the ordinary rate. The
`max` overload exists because that is nearly always the wrong thing to do by accident.

## Scheduling

A message delivered later, with no scheduler process, no cron and no broker plugin: the
library moves the message through a ladder of delay queues in the broker.

`Scheduler.OnAsync(mq)` declares that ladder, so it is built once — the singleton shape
above, verbatim. `InAsync(delay, exchange, routingKey, payload)` and
`AtAsync(when, exchange, routingKey, payload)` are the whole surface, plus `Scheduled`,
`Delivered` and `Hops` counters worth putting on a dashboard.

A scheduled message arrives at its target exchange as an ordinary message, so **the thing
that receives it is an ordinary `AddConsumer`**. That is the half of scheduling this package
does configure:

```json
{
  "acemq": {
    "topology": {
      "exchanges": [{ "name": "reminders", "type": "topic" }],
      "queues": [{ "name": "reminders.due", "deadLetter": true }],
      "bindings": [
        { "queue": "reminders.due", "exchange": "reminders", "routingKey": "reminder.due" }
      ]
    }
  }
}
```

```csharp
builder.Services
    .AddAceMq(builder.Configuration.GetSection("acemq"))
    .AddConsumer<Reminder, ReminderHandler>("reminders.due");
```

The broker's own delayed-message plugin is not needed and is not used.
`Capability.DelayedDelivery` reports whether the transport has native support; the ladder
does not depend on it.

## Sagas

A saga here is **not a messaging construct at all**, and that is the surprise worth getting
out of the way. `Saga<T>` takes steps and compensations as delegates and runs them in
process; nothing about it touches the connection.

```csharp
builder.Services.AddScoped<PlaceOrder>();

public sealed class PlaceOrder
{
    private readonly IStock _stock;
    private readonly IPayments _payments;

    public PlaceOrder(IStock stock, IPayments payments)
    {
        _stock = stock;
        _payments = payments;
    }

    public Task<SagaResult> RunAsync(Order order, CancellationToken ct) =>
        Saga<Order>.Named("place-order")
            .Step("reserve", (o, c) => _stock.ReserveAsync(o, c))
                .CompensateWith((o, c) => _stock.ReleaseAsync(o, c))
            .Step("charge", (o, c) => _payments.ChargeAsync(o, c))
                .CompensateWith((o, c) => _payments.RefundAsync(o, c))
            .Build()
            .RunAsync(order, ct);
}
```

So a saga is an ordinary scoped service, injected into a handler like any other, and the
handler decides what its `SagaResult` means:

```csharp
public async Task<Ack> HandleAsync(IMessage<Order> message, CancellationToken ct)
{
    var result = await _saga.RunAsync(message.Payload, ct);

    if (result.IsComplete) return Ack.Accept();

    // Compensated: the work was undone, so the message has nothing left to do. Retrying it
    // would run the whole saga again.
    if (!result.HasUnresolved) return Ack.DeadLetter($"saga failed at {result.FailedAt}");

    // A compensation itself failed. Nothing automatic can fix that.
    return Ack.Park($"saga left {result.Unresolved.Count} step(s) unresolved");
}
```

`result.HasUnresolved` is the distinction that matters and the one a first attempt gets
wrong: a saga that compensated cleanly failed *safely*, and a saga whose compensation threw
has left the world half-changed. The first is a dead letter, the second is
[`Ack.Park`](retries.md#what-a-handler-can-decide) and a person.

Pass the handler's cancellation token through. It stays uncancelled for the whole of a
graceful drain, so a saga in flight when shutdown begins finishes its steps rather than
being torn off between two of them — which is precisely the state a saga exists to avoid.

## Pipelines

A pipeline is several steps, each on its own queue, with the message carrying a routing
slip that says where it goes next. The point of the queues is that a step can fail, be
retried and be **resumed at the step it failed on** rather than from the beginning.

`connection.Pipeline<T>(name)` returns a builder, and `BuildAsync()` declares the queues
and starts consuming them — so a pipeline **is** the work, not a description of it. That
makes it shape two, a hosted service, with one wrinkle: a pipeline is also the thing an
application sends into, so the hosted service has to expose it.

```csharp
builder.Services.AddSingleton<Fulfilment>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<Fulfilment>());

public sealed class Fulfilment : IHostedService
{
    private readonly IAceMqConnectionProvider _connections;
    private Pipeline<Order>? _pipeline;

    public Fulfilment(IAceMqConnectionProvider connections) => _connections = connections;

    public async Task StartAsync(CancellationToken ct)
    {
        var mq = await _connections.GetAsync(ct);
        _pipeline = await mq.Pipeline<Order>("fulfilment")
            .Prefetch(20)
            .WithRetry(RetryPolicy.Exponential(5, TimeSpan.FromSeconds(1)))
            .Step<Reserved>("reserve", async order => await Reserve(order))
            .Step<Charged>("charge", async reserved => await Charge(reserved))
            .Step<Shipped>("ship", async charged => await Ship(charged))
            .BuildAsync();
    }

    public Task<string> SendAsync(Order order) => _pipeline!.SendAsync(order);

    public Task StopAsync(CancellationToken ct)
    {
        _pipeline?.Dispose();
        return Task.CompletedTask;
    }
}
```

The `AddSingleton` plus `AddHostedService(sp => ...)` pair is the ordinary .NET way to have
one instance that is both a service and hosted; `AddHostedService<T>()` alone registers a
second one. The example worker in this repository uses the same pair for its publisher.

A step returning `null` ends the run early rather than failing it — `Pipeline<T>.EndedEarly`
counts those. `ResumeAsync(payload, headers)` puts a message back **at the step named in
its slip**, which is what makes a dead-lettered pipeline message recoverable;
`Route.Of(message)` reads the slip off a message to find out where it was.

Because a pipeline drives its own consumers, none of `acemq:listener` applies to it.
Prefetch, retry and idempotency are on the builder, and they are the builder's own
defaults, not this package's.

## Ordered queues

`connection.Ordered<T>(name)` builds a queue whose messages are partitioned by a key and
handled in order within each partition — the tool for "order matters" that does not mean
"concurrency one".

It is shape two, and it is the one library type that registers itself as a health
contributor, so it appears in this package's health check as `check.{name}` with no wiring:
a halted partition shows up as `Degraded` on `/readyz` for free. See
[health checks](health.md#contributors).

`AddConsumer` cannot do this. Concurrency above one on an ordinary queue means messages are
no longer ordered, and that is not a setting an ordered queue can be reached through — see
[handlers](handlers.md#concurrency).

## Pulling one message

`PullAsync<T>(queue, timeout)` takes one message and hands it back unsettled, with
`AcknowledgeAsync()` and `RejectAsync(requeue)` on it. It is for the cases a subscription is
wrong for: a command-line tool, a test, an endpoint that drains a queue on demand.

Do not build a consumer out of it. A loop that pulls is a loop that polls, and the whole
reason `AddConsumer` exists is that the broker pushes.

## Back pressure, and a blocked broker

Two different things with one name, and this package handles them differently.

**Back pressure on publishing** is configuration: `acemq:maxOutstandingPublishes` (10,000)
bounds how many publishes may be awaiting a confirm at once, and `acemq:confirmTimeout`
(30s) bounds how long each waits. Together they are what stops a producer outrunning a
broker until the process runs out of memory. See [publishing](publishing.md).

**A blocked connection** is the broker protecting itself, and it is not configuration at
all. The library has **no event and no callback** for it — there is nothing to subscribe to.
It is observed by reading `IsBlocked` and `BlockedReason`, and this package already does
that on your behalf: a blocked connection appears in the health check's data as
`blocked` with `blockedReason` beside it, reported **healthy, with the reason**. Why healthy
is on the [health page](health.md#a-blocked-connection-is-not-a-reason-to-restart), and it
is the most consequential decision in this package.

A publish attempted on a blocked connection eventually fails with
`ConnectionBlockedException`, which carries `Reason`. If you would rather stop trying than
queue up failures, `PausePublishing()` and `ResumePublishing()` are on the connection, and
a publish attempted while paused throws `PublishingPausedException` rather than blocking.
Nothing in this package calls either; a pause is a decision about the application's own
behaviour.

If an alert is what you want, alert on `blocked == "true"` in the health data — noting that
[every value in it is a string](health.md#what-it-reports) — or on the broker's own
metrics, which is where the fact actually originates.
