# Request and reply

A question sent over a queue and the answer waited for. Two halves that are usually two
different services, and in a host they are two different shapes: the asking side is a
**singleton** and the answering side is a **hosted service**.

Neither is configuration. There is no `acemq:requestReply`, because a responder is a delegate
and a requester is an object something calls.

## Before reaching for it

A request-reply over a queue is a synchronous call with more moving parts than an HTTP one. It
buys three things — the broker's routing, the broker's queueing while the responder is
restarting, and one transport for a service that already has AMQP — and it costs a reply queue
per requester, a timeout you have to choose, and a call that can fail in a way HTTP does not.

If both ends are yours and both are up at the same time, HTTP is usually the simpler answer.
This pattern earns its place when the responder is a fleet, when the request should queue
rather than fail, or when the reply has to cross a network that only carries AMQP.

## Asking

`connection.RequesterAsync()` returns a `Requester`, and **a `Requester` declares a reply queue
of its own** — `acemq.reply.` plus a fresh GUID. So there must be one per process and not one
per call: a `Requester` per request leaves a queue on the broker per request ever made, with
no name by which anything could find them later.

That makes it the singleton shape, built on first use because `RequesterAsync` is async and a
constructor is not:

```csharp
builder.Services.AddSingleton<Pricing>();

public sealed class Pricing : IDisposable
{
    private readonly IAceMqConnectionProvider _connections;
    private readonly SemaphoreSlim _gate = new SemaphoreSlim(1, 1);
    private Requester? _requester;

    public Pricing(IAceMqConnectionProvider connections) => _connections = connections;

    public async Task<Quote> QuoteAsync(Basket basket, CancellationToken ct)
    {
        var requester = await RequesterAsync(ct);

        return await requester.RequestAsync<Basket, Quote>(
            "pricing", "quote.requested", basket, TimeSpan.FromSeconds(3), ct);
    }

    private async Task<Requester> RequesterAsync(CancellationToken ct)
    {
        var existing = _requester;
        if (existing != null) return existing;

        await _gate.WaitAsync(ct);
        try
        {
            return _requester ??= await (await _connections.GetAsync(ct)).RequesterAsync();
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose()
    {
        _requester?.Dispose();
        _gate.Dispose();
    }
}
```

A singleton rather than a hosted service **on purpose**: container singletons are disposed
after every hosted service has stopped, so a handler still finishing its message during a
drain can still ask a question. A `Requester` owned by a hosted service registered after
`AddAceMq` would already be disposed — see
[the two shapes](patterns.md#the-two-shapes), where this is the trap.

### From ASP.NET Core

An ordinary injected service, with the request's own cancellation token:

```csharp
app.MapPost("/quote", async (Basket basket, Pricing pricing, CancellationToken ct) =>
{
    try
    {
        return Results.Ok(await pricing.QuoteAsync(basket, ct));
    }
    catch (RequestTimedOutException)
    {
        // 504 rather than 500: nothing is broken here, the answer did not arrive in time.
        return Results.StatusCode(StatusCodes.Status504GatewayTimeout);
    }
});
```

`CancellationToken` in a minimal API handler is `HttpContext.RequestAborted`, so a client that
gives up stops the wait rather than leaving it to time out. Pass it; the three-argument
`RequestAsync` overload does not take one and defaults to thirty seconds, which is longer than
most HTTP clients will wait.

**Choose a timeout shorter than the caller's.** A three-second request behind a
thirty-second gateway timeout fails in a way the caller can read; the other way round, the
caller gives up first and the reply arrives to nobody. `Requester.Unmatched` counts exactly
that — a reply that came back with no one waiting for it — and a rising `Unmatched` means the
timeout is too short, not that the responder is broken.

`Requester.TimedOut` is the other counter and is the one to alert on.

### It is a synchronous call in a handler

A consumer that asks a question holds its prefetch slot and its concurrency slot for the whole
round trip, and **the drain waits for it**. A handler that makes a three-second request has a
drain at least three seconds long per in-flight message; a handler that makes one with the
default thirty-second timeout against a responder that is down has a drain that fails.

So: a short explicit timeout, always, and `concurrency` high enough that a slow responder does
not stall the queue behind it. See
[startup and shutdown](lifecycle.md#what-a-drain-finishes-precisely).

## Answering

`connection.RespondAsync<TRequest, TResponse>(queue, handler)` consumes a queue, calls the
handler, and publishes the return value back to wherever the request said. It is work that
drives itself, so it is a hosted service:

```csharp
builder.Services
    .AddAceMq(builder.Configuration.GetSection("acemq"));

builder.Services.AddHostedService<QuoteResponder>();

public sealed class QuoteResponder : IHostedService
{
    private readonly IAceMqConnectionProvider _connections;
    private readonly IServiceProvider _services;
    private Responder? _responder;

    public QuoteResponder(IAceMqConnectionProvider connections, IServiceProvider services)
    {
        _connections = connections;
        _services = services;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var mq = await _connections.GetAsync(cancellationToken);
        _responder = await mq.RespondAsync<Basket, Quote>("pricing.quotes", Answer);
    }

    private async Task<Quote> Answer(Basket basket)
    {
        // A scope per request, because nothing makes one here. This is the difference
        // from AddConsumer that catches people.
        using var scope = _services.CreateScope();
        var prices = scope.ServiceProvider.GetRequiredService<PriceBook>();
        return await prices.QuoteAsync(basket);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _responder?.Dispose();
        return Task.CompletedTask;
    }
}
```

Three differences from `AddConsumer`, all of them worth knowing:

- **The handler takes the payload, not an `IMessage<T>`.** No envelope, no attempt number, no
  routing key. If the answer depends on any of those, this is the wrong tool and an ordinary
  consumer that publishes its own reply is the right one.
- **There is no `Ack`.** Returning a value answers; throwing does not answer, and the request
  times out at the caller. There is no dead letter and no retry ladder for a request.
- **No scope is created.** `AddConsumer` gives a handler a fresh `IServiceScope` per message;
  nothing does that here, so a responder touching scoped services makes its own. Without it a
  `DbContext` is shared across every request, which fails at the second concurrent one.

`RespondAsync` has a second overload taking `ConsumerOptions`, which is where prefetch goes:

```csharp
_responder = await mq.RespondAsync<Basket, Quote>(
    "pricing.quotes", ConsumerOptions.Prefetch(50), Answer);
```

Nothing under `acemq:listener` applies to a responder — not prefetch, not concurrency, not the
retry ladder. It is not a consumer this package started.

### The queue it answers on

Declare it in `acemq:topology` like any other, so it exists before the responder starts:

```json
{
  "acemq": {
    "topology": {
      "exchanges": [{ "name": "pricing", "type": "topic" }],
      "queues": [{ "name": "pricing.quotes" }],
      "bindings": [
        { "queue": "pricing.quotes", "exchange": "pricing", "routingKey": "quote.requested" }
      ]
    }
  }
}
```

`AceMqConnectionHost` applies the topology before any hosted service of yours starts, which is
the ordering that makes this safe.

### Counters

`Answered` and `Unanswerable` on the `Responder`, `IsRunning` for whether it still is.
`Unanswerable` counts a request that arrived with no reply address — a message published to
the same queue by something that was not a requester, which is usually a routing key bound
twice by mistake.

## Shutdown

A responder registered after `AddAceMq` stops **before** the consumers drain, which is right:
it stops answering before the instance leaves the rotation, and requests queue up for the next
instance rather than being answered by one that is going away.

An in-flight answer is cut off by `Dispose()` rather than waited for. If a half-finished answer
leaves anything behind, wait for it in `StopAsync` before disposing — there is no equivalent of
the consumer host's [drain](lifecycle.md#what-a-drain-finishes-precisely) for a responder.

The reply queue a `Requester` owns goes away with the connection. It is named for a GUID and is
never reused, so nothing accumulates on the broker across restarts — but a process that creates
requesters in a loop will accumulate them within one run, which is the reason for the singleton
at the top of this page.

## Metrics

Two instruments, both reaching an [OpenTelemetry](observability.md) pipeline with no wiring:

| | |
|---|---|
| `acemq.request.duration` | round-trip time, tagged by target |
| `acemq.request.total` | requests, tagged by outcome — `answered` or `timed_out` |

The ratio of those two outcomes is the health of the pair, and it is more useful than either
side's own logs, because a timeout is the one failure both ends see differently.

## Testing it

Both halves over `memory://`, in one process, with no broker:

```csharp
var mq = host.Services.GetRequiredService<AceMqConnection>();

using var responder = await mq.RespondAsync<Basket, Quote>(
    "pricing.quotes", basket => Task.FromResult(new Quote(basket.Id, 42m)));
using var requester = await mq.RequesterAsync();

var quote = await requester.RequestAsync<Basket, Quote>(
    "pricing", "quote.requested", new Basket("b-1"), TimeSpan.FromSeconds(5), default);

Assert.Equal(42m, quote.Total);
```

The test worth adding beside it is the timeout: no responder, a 200 ms deadline, and
`Assert.ThrowsAsync<RequestTimedOutException>`. That path is the one an application actually
has to handle, and it is the one nobody writes a test for.
