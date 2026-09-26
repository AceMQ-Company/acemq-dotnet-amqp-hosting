# Publishing

This package configures a connection and runs consumers. Publishing is the library's, and
the connection you inject is the library's own — so everything on
[the library's publishing page](https://acemq.org/acemq-dotnet-amqp/publishing.html)
applies unchanged.

## From a service

```csharp
public sealed class Orders
{
    private readonly AceMqConnection _mq;

    public Orders(AceMqConnection mq) => _mq = mq;

    public async Task PlaceAsync(Order order)
    {
        var result = await _mq.Publisher<Order>("orders", "order.created").SendAsync(order);
        if (!result.Routed)
        {
            // The broker accepted it and no queue was bound to take it. Confirms catch
            // that; nothing else does.
        }
    }
}
```

`AceMqConnection` is a singleton, and `Publisher<T>` is cheap — but not free. A publisher
created per call re-reads the interceptor list each time; one held in a field does not.
Either is fine; the field is better in a hot path.

## From a BackgroundService

Use the provider rather than the connection, and await rather than block:

```csharp
protected override async Task ExecuteAsync(CancellationToken stoppingToken)
{
    var mq = await _connections.GetAsync(stoppingToken);
    var publisher = mq.Publisher<Order>("orders", "order.created");
    // ...
}
```

## Confirms and back pressure

On by default. `acemq:maxOutstandingPublishes` (10,000) bounds how many publishes can be
awaiting a confirm at once, and `acemq:confirmTimeout` (30s) bounds how long each waits.
Both are the back pressure that stops a producer outrunning a broker until the process runs
out of memory.

A broker that has *blocked* the connection is a different thing with a similar name, and it
is not configuration at all — there is no event to subscribe to, and this package reports it
through the health check instead. See
[back pressure and a blocked broker](patterns.md#back-pressure-and-a-blocked-broker).

## Publishing at shutdown

**A publish waiting for its confirm is not part of the drain.** Nothing waits for it, and a
connection closing under it will fail it. See [startup and shutdown](lifecycle.md).

The practical shape for a service that both publishes and consumes:

- Register the publishing `BackgroundService` **after** `AddAceMq`. Hosted services stop in
  reverse order, so it stops before the consumers drain, and the drain is not racing new
  publishes.
- If a publish must survive a restart, it belongs in the library's outbox rather than in a
  shutdown hook. `connection.Outbox(store)` is the entry point, and
  [the outbox page](outbox.md) is the whole arrangement — including why an outbox is the one
  publishing mechanism that does not care about shutdown ordering.

## Publishing from a handler

Ordinary. A handler that publishes during a drain is publishing on a connection that is
still open — the connection is closed after the drain, not before — so the publish
completes. That is the reason the cancellation of the handler token is the last thing the
drain does and not the first: once it is cancelled, a handler that wants to dead-letter a
message can no longer republish it, and the delivery is nacked instead.
