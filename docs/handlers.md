# Handlers and consumers

## The interface

```csharp
public interface IMessageHandler<T>
{
    Task<Ack> HandleAsync(IMessage<T> message, CancellationToken cancellationToken);
}
```

```csharp
builder.Services
    .AddAceMq(builder.Configuration.GetSection("acemq"))
    .AddConsumer<Order, OrderHandler>("orders.new");
```

`AddConsumer` registers the handler type in the container for you, scoped by default, and
records a consumer to start when the host starts.

## Why an interface and not an attribute

The Spring Boot starter turns an annotated method into a consumer. That is the right answer
there: Spring's whole model is a bean post-processor finding annotations on beans that the
container already owns, and an application that uses Spring is fluent in it.

.NET is not that. There is no ambient scanning phase, attribute discovery means either
reflecting over every loaded assembly at startup or asking the application to list its
assemblies, and neither is something a .NET application expects to pay for. What a .NET
application does expect is a type it can register:

- **Constructor injection works, with no special case.** A handler takes an
  `ILogger<T>`, an `IOptions<T>`, a `DbContext`, an `HttpClient` from
  `IHttpClientFactory` — all of it resolved the way everything else in the application is.
- **A scope per message.** `AddConsumer` creates an `IServiceScope` for each delivery, so
  scoped services behave exactly as they do per-request in ASP.NET Core, and two messages
  handled concurrently never share one.
- **Testing is calling a method.** `new OrderHandler(logger).HandleAsync(message, default)`
  needs no host, no container and no broker.
- **It is checked at compile time.** `AddConsumer<Order, OrderHandler>` does not compile
  unless `OrderHandler` really does handle `Order`. An attribute on a method with the wrong
  parameter type is a startup failure at best.
- **Decorators, interceptors and `TryAddEnumerable` all apply**, because the handler is a
  normal registration, and this package does not have to know that they exist.

The cost is one more file than a method with an attribute on it, and a name for the class.
That seems a fair price for everything above.

## A delegate, for the three-line case

```csharp
builder.Services
    .AddAceMq()
    .AddConsumer<Order>("orders.cancelled", async (message, ct) =>
    {
        await audit.RecordAsync(message.Payload.Id, ct);
        return Ack.Accept();
    });
```

Invoked directly, with no scope created around it — there is nothing to resolve. A delegate
that wants a scoped service should capture `IServiceProvider` and make its own scope, or be
a handler type instead.

## Handler lifetime

```csharp
.AddConsumer<Order, OrderHandler>("orders.new", lifetime: ServiceLifetime.Singleton)
```

Scoped is the default and is almost always right. Singleton is for a handler that must keep
state between messages, and makes thread safety its own problem. Transient gets a new
handler per message but still inside the scope, which is rarely what anyone wants and is
there for completeness.

## What a handler returns

`Ack` is the library's, and the four answers are the library's:

| | |
|---|---|
| `Ack.Accept()` | Done. Acknowledged. |
| `Ack.Retry(reason)` or `Ack.Retry(after, reason)` | Try again, through the ladder if one is configured. |
| `Ack.DeadLetter(reason)` | This message will fail the same way forever; file it. |
| `Ack.Release()` | Not now; give it back to the broker for somebody else. |
| `Ack.Park(reason)` | Set it aside for a human, separately from the dead letters. |

Throwing is also allowed and is treated as a failure by the library's rules — retried if a
ladder is configured, dead-lettered otherwise, or requeued if `listener:requeueOnFailure`
is set.

## The cancellation token

**It is not the host's stopping token**, and that is the single most important thing on
this page.

A handler given the host's stopping token sees it cancelled the moment shutdown begins —
so it aborts, and the drain that was meant to let it finish is the thing that killed it.
The token here stays uncancelled through a graceful drain and is cancelled only once the
drain has already overrun, which is the signal to stop and let the broker redeliver.

Pass it to whatever you call. A handler that ignores it entirely still finishes on a
graceful drain; it just cannot be stopped when the drain fails, and then the host's own
deadline is what ends the process.

[Startup and shutdown](lifecycle.md) has the rest.

## Per-consumer settings

```csharp
.AddConsumer<Order, OrderHandler>("orders.new", configure: r =>
{
    r.Prefetch = 10;
    r.Concurrency = 4;
    r.AutoStartup = false;
    r.Retry = new AceMqRetryOptions { Enabled = true, MaxAttempts = 5 };
    r.RequeueOnFailure = false;
    r.Idempotency = sp => sp.GetRequiredService<IIdempotencyStore>();
})
```

Anything left unset falls through to the [listener defaults](configuration.md#listener-defaults)
— except `Idempotency`, which has no listener default and no configuration key, because a
store is a service rather than a scalar. [Retries and duplicates](retries.md) has the whole
story, and it is the page to read next if a handler changes anything.

## Naming, and starting one by hand

Every consumer has a name — the handler's type name, or the queue for a delegate, or
whatever `name:` says. Two consumers with the same name are refused when the host starts,
because the name is how they are told apart in logs and in `AceMqConsumerHost.Running`.

A consumer registered with `AutoStartup = false` sits idle until asked:

```csharp
var consumers = host.Services.GetRequiredService<AceMqConsumerHost>();
await consumers.StartAsync("OrderHandler");
```

## Concurrency

Every consumer is a `ConsumerGroup`, even at concurrency one, as in the Spring starter. One
type means one shutdown path and one set of counters to read, and the group of one costs
nothing.

Concurrency above one means messages from that queue are no longer handled in order. If
order matters, leave it at one and scale by partitioning — the library's `Ordered` builder
is the tool for that, and it is reachable from the injected `AceMqConnection`. See
[ordered queues](patterns.md#ordered-queues).

## Consuming something that is not a queue

`AddConsumer` consumes a queue, and two things that look like queues are not:

- **A stream** needs an offset, and there is nowhere in `AddConsumer` to put one. See
  [streams](streams.md).
- **A request-reply queue** needs the handler's return value published back to the caller.
  See [request and reply](request-reply.md).

Both are a hosted service holding the library's own type, and both are on
[patterns from a host](patterns.md).
