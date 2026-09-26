# The transactional outbox

A service writes a row and publishes a message, and the process dies between the two. Either
the row exists and nothing was announced, or the message went out about a row that was rolled
back. There is no ordering of those two writes that fixes it, because they are writes to two
different systems.

The outbox makes them one write. The message is inserted into a table **in the same
transaction as the work**, and a relay publishes it afterwards. The transaction is the whole
mechanism; everything else on this page is plumbing around it.

## It is not configured

There is no `acemq:outbox` and there will not be one. An outbox is a database connection, a
table name and a polling cadence, and this package does not open database connections. So
this is hand-wiring, in the two shapes the [patterns page](patterns.md#the-two-shapes)
describes: the **store** is an ordinary singleton, and the **relay** is a hosted service.

## The store

`IOutboxStore` is five methods and two implementations ship with the library.
`DbOutboxStore` is the one that means anything:

```csharp
builder.Services.AddSingleton<IOutboxStore>(_ =>
    new DbOutboxStore(() => new NpgsqlConnection(connectionString)));
```

The argument is a `ConnectionSupplier` — `delegate DbConnection ConnectionSupplier()` — so any
ADO.NET provider works. `CreateTableSql()` gives the DDL for a migration; the table and the
parameter prefix are the second and third arguments for a database whose placeholders are not
`@`:

```csharp
// Print it once and paste it into a migration, rather than calling it at startup.
Console.WriteLine(new DbOutboxStore(supplier).CreateTableSql());
```

`InMemoryOutboxStore` **is not an outbox**, and the library's own documentation says so. It
shares the process's lifetime, so it cannot be written in the same transaction as anything
durable and everything in it is lost on a restart — which is precisely the failure the pattern
exists to prevent. It is for exercising a relay in a test.

## The write that makes it work

`DbOutboxStore.AddAsync(record, transaction)` is the overload the pattern is about. The
message and the work go into the database together:

```csharp
public sealed class Orders
{
    private readonly ConnectionSupplier _connections;
    private readonly IOutboxStore _outbox;
    private readonly AceMqConnection _mq;

    public Orders(ConnectionSupplier connections, IOutboxStore outbox, AceMqConnection mq)
    {
        _connections = connections;
        _outbox = outbox;
        _mq = mq;
    }

    public async Task PlaceAsync(Order order, CancellationToken ct)
    {
        using var connection = _connections();
        await connection.OpenAsync(ct);
        using var transaction = await connection.BeginTransactionAsync(ct);

        await Insert(order, connection, transaction, ct);

        // The message, in the same transaction. Nothing has been published yet and nothing
        // needs to have been: if this transaction commits, the message will go out, and if
        // it rolls back, the message never existed.
        await ((DbOutboxStore)_outbox).AddAsync(
            OutboxRecord.For(_mq, "orders", "order.created", order),
            transaction);

        await transaction.CommitAsync(ct);
    }
}
```

Two things in there.

**`OutboxRecord.For` takes the connection**, and it is not decoration — the record stores the
payload already encoded, so the record has to be built with the codec the connection was
opened with. That is what keeps the row's contents and what eventually goes on the wire the
same thing. A `For` overload takes an `Envelope` as well, for a correlation id that should
match the work's.

**The cast is the honest part.** `IOutboxStore.AddAsync` has no transaction parameter, because
not every store has transactions; the overload that does is on `DbOutboxStore`. So a service
doing this depends on the concrete type, and it may as well say so in its constructor:

```csharp
public Orders(ConnectionSupplier connections, DbOutboxStore outbox, AceMqConnection mq)
```

```csharp
builder.Services.AddSingleton<DbOutboxStore>(_ =>
    new DbOutboxStore(() => new NpgsqlConnection(connectionString)));
builder.Services.AddSingleton<IOutboxStore>(sp => sp.GetRequiredService<DbOutboxStore>());
```

Registering both is what lets the writer take the concrete type and the relay take the
interface, which is the right split: the writer needs the transaction, the relay does not.

## The relay

`connection.Outbox(store)` builds an `OutboxRelay`, which claims a batch of unpublished
records, publishes them, and marks them. The lease on the claim is what stops two instances
publishing the same message.

```csharp
builder.Services
    .AddAceMq(builder.Configuration.GetSection("acemq"))
    .AddConsumer<Order, OrderHandler>("orders.new");

// After AddAceMq: starts once the connection is open.
builder.Services.AddHostedService<OutboxPublisher>();

public sealed class OutboxPublisher : BackgroundService
{
    private readonly IAceMqConnectionProvider _connections;
    private readonly IOutboxStore _store;
    private readonly ILogger<OutboxPublisher> _log;

    public OutboxPublisher(
        IAceMqConnectionProvider connections,
        IOutboxStore store,
        ILogger<OutboxPublisher> log)
    {
        _connections = connections;
        _store = store;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var mq = await _connections.GetAsync(stoppingToken);
        using var relay = mq.Outbox(_store);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var published = await relay.DrainOnceAsync();
                if (published > 0)
                {
                    _log.LogDebug("outbox published {Count}", published);
                    // Straight round again rather than waiting: a backlog should drain at
                    // the speed of the broker, not of the poll interval.
                    continue;
                }
            }
            catch (Exception e)
            {
                // A broker that is down is not a reason to stop relaying for ever. The rows
                // are still there; the next pass will find them.
                _log.LogWarning(e, "the outbox relay pass failed");
            }

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
```

A `BackgroundService` here rather than an `IHostedService`, because this genuinely is a loop —
which is the distinction the [patterns page](patterns.md#shape-two-a-hosted-service-that-drives-work)
draws.

`OutboxRelay.Start()` is the shorter alternative and starts a poll loop of its own, stopped by
`Dispose()`. The loop above is longer and is worth it: the cadence, the logging and the
cancellation are the host's rather than the library's, and a failed pass is a log line you
wrote rather than one you did not.

`Outbox(store)` also takes a batch size, a poll interval and a lease through the
`OutboxRelay` constructor — `new OutboxRelay(mq, store, batchSize: 500, pollInterval:
TimeSpan.FromSeconds(1), lease: TimeSpan.FromSeconds(30))` — and the defaults are 100, one
second and thirty seconds. The lease has to be comfortably longer than a batch takes to
publish, or a second instance claims rows the first is still working on.

## Why the relay may stop first, and that being fine

Hosted services stop in reverse registration order, so `OutboxPublisher` — registered after
`AddAceMq` — stops **before** `AceMqConsumerHost` drains. For most hand-wired things that is
the trap the [patterns page](patterns.md#the-two-shapes) warns about. Here it is harmless, and
the reason is the point of the whole pattern:

**A handler that writes to the outbox during a drain does not need the relay.** It writes a
row and commits. The relay is gone, nothing is published, and nothing is lost — the row is
there, and the next instance to run a pass finds it. An outbox is the one publishing mechanism
that is *indifferent* to shutdown ordering.

Which is also the answer to the awkward row in the
[drain table](lifecycle.md#what-a-drain-finishes-precisely): a publish waiting for its confirm
is not part of the drain and is cut off. If a message must survive a restart, it belongs here
rather than in a shutdown hook.

## Health and metrics

`PendingCountAsync()` is the number that matters, and a growing one is the signal that the
relay has stopped or that the broker is refusing. As a health contributor it takes the instance
out of the readiness rotation before anybody notices by other means:

```csharp
sealed class OutboxHealth : IHealthContributor
{
    private readonly IOutboxStore _store;

    public OutboxHealth(IOutboxStore store) => _store = store;

    public string Name => "outbox";

    public HealthReport Report()
    {
        // IHealthContributor.Report is synchronous and PendingCountAsync is not, which is
        // the awkward join. Blocking is safe here — there is no synchronization context —
        // and a cached value refreshed by the relay loop is better still if the count is
        // expensive on your database.
        var pending = _store.PendingCountAsync().ConfigureAwait(false).GetAwaiter().GetResult();

        return new HealthReport(
            Name,
            pending > 10_000 ? AceMq.Amqp.HealthStatus.Degraded : AceMq.Amqp.HealthStatus.Up,
            new Dictionary<string, string>
            {
                ["pending"] = pending.ToString(CultureInfo.InvariantCulture),
            });
    }
}
```

Registered with `connection.RegisterHealth(new OutboxHealth(store))` from the same hosted
service that builds the relay, it appears in this package's health check as `check.outbox`,
and `Degraded` there makes the whole check `Degraded`. See
[health checks](health.md#contributors).

`AceMq.Amqp.HealthStatus` is written out in full above on purpose: it collides with
`Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus`, and inside a namespace under
`AceMq.Amqp` the enclosing namespace beats a file-level `using` alias. That collision has cost
this package a debugging session and is documented on the library's enum.

The library publishes two instruments of its own, and both reach an
[OpenTelemetry](observability.md) pipeline with no extra wiring:

| | |
|---|---|
| `acemq.outbox.lag` | how long a record waited between being written and being published |
| `acemq.outbox.total` | records published, tagged by outcome |

Lag is the better alert. A count tells you the relay is running; lag tells you whether it is
keeping up.

## Ordering, and what an outbox does not promise

Records are claimed in batches and published in the order they were claimed, but **nothing
here guarantees global ordering**. Two instances relaying at once publish two batches
concurrently, and a failed publish is retried on a later pass, after messages written later
have already gone out.

If the order of two messages matters, an outbox is not the mechanism —
[ordered queues](patterns.md#ordered-queues) are, and they work on the consuming side where
ordering can actually be enforced.

Nor does an outbox give you exactly-once. A record published and then not marked — the process
dying in that window — is published again by the next pass. The outbox turns "maybe never"
into "at least once", and the other half of the problem is on the consumer:
[handling a message once](retries.md#handling-a-message-once).

## Testing it

`InMemoryOutboxStore` plus the `memory://` transport tests the relay end to end with no broker
and no database — it has a `Pending()` method for asserting on:

```csharp
var store = new InMemoryOutboxStore();
await store.AddAsync(OutboxRecord.For(mq, "orders", "order.created", new Order("o-1")));

var published = await mq.Outbox(store).DrainOnceAsync();

Assert.Equal(1, published);
Assert.Empty(store.Pending());
```

What that cannot test is the transaction, which is the part that matters. For that there is no
substitute for a real database: write a row and a record in one transaction, roll it back, and
assert that `PendingCountAsync()` is zero. A suite that only ever commits is a suite that
would pass if `AddAsync` ignored its transaction argument.
