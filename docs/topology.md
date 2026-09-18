# Topology

Exchanges, queues and bindings declared in configuration and applied at startup, before any
consumer starts.

```json
{
  "acemq": {
    "topology": {
      "apply": "Create",
      "failOnDrift": false,
      "exchanges": [
        { "name": "orders", "type": "topic" }
      ],
      "queues": [
        {
          "name": "orders.new",
          "type": "Quorum",
          "deadLetter": true,
          "arguments": { "x-max-length": "100000" }
        }
      ],
      "bindings": [
        { "queue": "orders.new", "exchange": "orders", "routingKey": "order.created" }
      ]
    }
  }
}
```

## Nothing is declared unless something is written

An empty `topology` section declares nothing at all. This package will not guess a queue
name from a consumer registration, because a typo in a queue name should fail loudly rather
than quietly create the queue it was a typo for.

## `apply`

| | |
|---|---|
| `Create` | declare what is missing. The default, once anything is declared. |
| `DryRun` | declare nothing; log the plan. What to run against production before running `Create` against it. |
| `None` | declare nothing and say nothing. The declarations become documentation. |

## Exchanges

`type` is `topic`, `direct`, `fanout` or `headers`, and defaults to `topic`.

## Queues

`type` is `Classic`, `Quorum` or `Stream`, and defaults to **`Quorum`**. That default is
deliberate: a classic queue on a cluster acknowledges a message that one node has, and that
node can be the one that dies.

`deadLetter: true` declares `{name}.dlq` alongside and points this queue's dead letters at
it.

`arguments` are passed to the broker as declared, with one convenience: a value that parses
as an integer is sent as one, and `true`/`false` as a boolean. RabbitMQ rejects
`x-max-length` as the string `"100000"` with an error that names the type and not the
setting, and a JSON configuration file has only strings to offer.

Every consumer also declares `{queue}.dlq` and `{queue}.parked` for itself, plus the rungs
of its retry ladder when it has one — that is the library's behaviour, not this package's,
and it happens whether or not the queue is declared here.

## Bindings

`routingKey` defaults to the empty string, which is what a fanout exchange wants and what a
topic exchange almost never does.

## Drift

Drift is a queue or exchange that exists with settings a declaration cannot change — a
classic queue where the configuration asks for quorum, different arguments, a different
exchange type. Redeclaring over it is not something the broker offers, so the only question
is whether to say so and carry on or to refuse to start.

By default it is logged at warning and the application starts. `failOnDrift: true` makes it
refuse, which is the right setting for an environment where the topology is supposed to be
under this application's control and a difference means somebody changed it by hand.

The plan is rendered into the log either way, so the warning says what differs rather than
that something does.

## Declaring more than configuration can express

The injected `AceMqConnection` has the library's whole topology surface on it — streams,
retry ladders with a named policy, arguments that are not scalars. A hosted service
registered **after** `AddAceMq` runs after the connection host has opened the connection:

```csharp
builder.Services.AddAceMq(builder.Configuration.GetSection("acemq"));
builder.Services.AddHostedService<ExtraTopology>();

sealed class ExtraTopology(IAceMqConnectionProvider connections) : IHostedService
{
    public async Task StartAsync(CancellationToken ct)
    {
        var mq = await connections.GetAsync(ct);
        await mq.DeclareStreamAsync("events", TimeSpan.FromDays(7), maxLengthBytes: null);
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
```

Registered after `AddAceMq` but before any consumer needs the result — consumers start from
`AceMqConsumerHost`, which `AddAceMq` registered second, so anything added later starts
after it. If a consumer must find a queue this code declares, declare it in configuration
instead.
