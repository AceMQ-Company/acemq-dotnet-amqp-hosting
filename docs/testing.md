# Testing

## A handler is a method

The reason for the typed-handler design shows up here first. A handler has no dependency on
this package beyond the interface, so testing one is calling it:

```csharp
[Fact]
public async Task It_accepts_a_valid_order()
{
    var handler = new OrderHandler(NullLogger<OrderHandler>.Instance);
    var ack = await handler.HandleAsync(Message(new Order("o-1", 9.99m)), default);
    Assert.True(ack.IsAccept);
}
```

No host, no container, no broker. `IMessage<T>` is an interface, so a stub or a mock will
do; the properties worth setting in one are `Payload`, `Attempt` and `Envelope`.

## A host over the in-memory transport

For the wiring rather than the logic, `memory://` gives a real transport — it declares,
binds, routes and redelivers — with no broker and no flake:

```csharp
var host = new HostBuilder()
    .ConfigureAppConfiguration(c => c.AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["acemq:url"] = "memory://tests",
        ["acemq:topology:queues:0:name"] = "orders.new",
        ["acemq:topology:exchanges:0:name"] = "orders",
        ["acemq:topology:bindings:0:queue"] = "orders.new",
        ["acemq:topology:bindings:0:exchange"] = "orders",
        ["acemq:topology:bindings:0:routingKey"] = "order.created",
    }))
    .ConfigureServices((ctx, s) => s
        .AddAceMq(ctx.Configuration.GetSection("acemq"))
        .AddConsumer<Order, OrderHandler>("orders.new"))
    .Build();

await host.StartAsync();

var mq = host.Services.GetRequiredService<AceMqConnection>();
await mq.Publisher<Order>("orders", "order.created").SendAsync(new Order("o-1", 9.99m));

// ... assert ...

await host.StopAsync();
```

**Give each test its own broker name.** `memory://a` and `memory://b` are separate brokers;
`memory://` twice is one broker shared between two tests, and the second one inherits the
first one's queues.

This package's own unit suite is built this way, including its drain tests — it is worth
reading if you want the shape.

What it cannot prove is that anything survives a socket, a broker that blocks, or a prefetch
window full of deliveries. For that there is no substitute for a real broker.

## A real broker

```bash
docker run -d --name acemq-test -p 5721:5672 rabbitmq:4-alpine
```

Point `acemq:url` at it and use the same host shape. Two notes from experience:

- **Serialize the tests.** They declare and delete topology by name, and two running at
  once are two processes agreeing on a queue and disagreeing about whether it should exist.
  In xUnit, a collection with `DisableParallelization = true`.
- **Do not add a skip path.** A suite that quietly does nothing when the broker is absent
  reports a green tick for exactly the thing nobody wants unverified.

## Turning it off

```json
{ "acemq": { "enabled": false } }
```

Nothing connects and no consumer runs, but every service is still registered, so nothing
fails to resolve and no test has to know which ones to stub. That is what a test host for
the *rest* of the application wants — and what a batch instance of the same application
wants in production.

## Testing the drain

The test worth having, in two halves:

```csharp
// The handler in flight finishes.
await host.StartAsync();
await Publish();
await entered.Task;           // the handler is running
await host.StopAsync();       // blocks on it
Assert.True(finished);

// A drain that overruns cancels rather than hanging.
// listener:shutdownTimeout of 500ms, a handler that waits five minutes:
var clock = Stopwatch.StartNew();
await host.StopAsync();
Assert.True(clock.Elapsed < TimeSpan.FromSeconds(30));
Assert.True(await handlerWasCancelled.Task);
```

Against a real broker, add the assertion that makes it mean something: after a successful
drain, `MessageCountAsync(queue)` is zero — the handler did not merely finish, its message
was acknowledged. After an overrun, it is *not* zero — the message came back rather than
being lost.
