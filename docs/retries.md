# Retries, dead letters and duplicates

Three things that are really one thing. A retry means a message arrives again; a message
arriving again means a handler runs twice; a handler that runs twice and changes something
needs to be able to tell. So the page that configures the first has to answer the third.

This is the one pattern area that **is** mostly configuration, and the settings table is on
the [configuration page](configuration.md#the-retry-ladder). This page is what they do.

## Off by default

A retry that is on by default is a retry nobody chose. Turning it on is one line:

```json
{
  "acemq": {
    "listener": {
      "retry": {
        "enabled": true,
        "maxAttempts": 5,
        "initialDelay": "00:00:01",
        "maxDelay": "00:05:00",
        "multiplier": 2.0,
        "jitter": 0.2,
        "brokerWaitThreshold": "00:00:05"
      }
    }
  }
}
```

That is a ladder of 1s, 2s, 4s, 8s — bounded at five minutes, jittered by a fifth, five
attempts including the first. `maxAttempts` counts the first attempt, so five means four
retries.

Durations are `TimeSpan`s and therefore written `"00:00:01"`, not `1s`. That is the
configuration binder's convention rather than a choice made here, and it is the first thing
to trip over when copying an `application.yaml` across from the Spring Boot starter.

### Per consumer

```csharp
builder.Services
    .AddAceMq(builder.Configuration.GetSection("acemq"))
    .AddConsumer<Order, OrderHandler>("orders.new")
    .AddConsumer<Email, EmailHandler>("email.outbound", configure: r =>
        r.Retry = new AceMqRetryOptions
        {
            Enabled = true,
            MaxAttempts = 10,
            InitialDelay = TimeSpan.FromSeconds(5),
            MaxDelay = TimeSpan.FromHours(1),
            BrokerWaitThreshold = TimeSpan.FromSeconds(5),
        });
```

`r.Retry` **replaces** the listener defaults for that consumer rather than merging with them,
because it is an object and not a set of nullable fields. A consumer that wants the default
ladder with one number changed has to write the ladder out — which is verbose and is at
least unambiguous. Anything left at its own default takes `AceMqRetryOptions`' default, not
the value in `appsettings.json`.

`giveUpAfter` is the one worth knowing about and is not in the example above. It gives up on
a message older than a duration however many attempts are left, which is what you want for
anything whose answer stops being useful — a notification nobody needs an hour late.

## Where the waiting happens

**The library does not sleep in the handler.** A retry republishes the message onto a delay
queue in the broker, and the broker delivers it back when the delay expires. That is the
whole point of the ladder: a waiting message holds nothing — no thread, no prefetch slot, no
delivery — and a restart does not have to survive the wait.

Except for the short ones. `brokerWaitThreshold`, thirty seconds by default, is the line:
delays **at or above it** wait on a rung queue in the broker, and delays below it wait in the
process.

That line is a **shutdown** setting as much as a reliability one, and it is the most
consequential number on this page:

- A wait spent in the process holds a delivery and a prefetch slot, **and the drain has to
  sit through it.** A consumer two seconds into a two-second wait makes the drain take the
  rest of it.
- A consumer five minutes into a five-minute wait — because that rung's queue was missing and
  it fell back to waiting here — makes the drain take five minutes, which is to say it makes
  the drain fail, because nothing grants a pod five minutes.

Lowering the threshold to five seconds moves nearly everything to the broker and is the
cheapest way to make a drain predictable. The full argument is on the
[lifecycle page](lifecycle.md#the-retry-waiting-out-a-backoff).

### The queues this declares

`AddConsumer` declares, per consumer, for you:

| | |
|---|---|
| `{queue}.dlq` | dead letters |
| `{queue}.parked` | messages set aside for a person |
| `{queue}.retry.{delay}` | one rung per broker-side delay in the ladder |

Plus the exchanges they hang off, `acemq.dlx` and `acemq.retry`. **This happens whether or
not the queue is in `acemq:topology`**, and it happens with no retry policy too — a consumer
with no ladder still needs somewhere to dead-letter, so `.dlq` and `.parked` appear
regardless. The [topology page](topology.md#queues) says the same from the other direction.

They are declared at startup rather than on first failure, deliberately: a queue an operator
can see from start-up is one they can alert on before the first failure rather than after it.

`acemq.retry.rung.missing` is the metric for a rung that was absent so the wait happened in
the process instead. It is worth an alert on its own merits and it is also the early warning
that the next deployment's drain will be slow. See
[observability](observability.md#what-is-counted).

## What a handler can decide

`Ack` is the library's, and each answer means something different to the ladder:

| | |
|---|---|
| `Ack.Accept()` | Done. Acknowledged, and nothing comes back. |
| `Ack.Retry(reason)` | Through the ladder, at whatever the next rung is. |
| `Ack.Retry(after, reason)` | Through the ladder, but wait exactly this long. |
| `Ack.DeadLetter(reason)` | **Skip the ladder.** This will fail the same way for ever. |
| `Ack.Release()` | Not now. Back to the broker unchanged, for somebody else. |
| `Ack.Park(reason)` | Set aside for a person, separately from the dead letters. |

Both `DeadLetter` and `Park` require a reason, and the requirement is not ceremony: the
reason is what a person reading `orders.new.parked` at nine in the morning has to work
from.

`Ack.DeadLetter` skipping the ladder is the one worth using more than people do. A message
whose payload will not deserialise, or that names a customer who does not exist, does not
become valid after four exponential delays — it consumes four attempts and arrives at the
same place five minutes later. Retrying is for things that might have changed: a timeout, a
lock, a rate limit, a dependency restarting.

### Throwing

Allowed, and treated as a failure by the library's own rules: retried if a ladder is
configured, dead-lettered if not — or requeued if `listener:requeueOnFailure` is set.

Returning an `Ack` is better where the handler knows which kind of failure it had, because
`Ack.DeadLetter("no such customer")` says something and an exception type usually does not.

### `requeueOnFailure`

```json
{ "acemq": { "listener": { "requeueOnFailure": true } } }
```

Puts a failed message straight back on the queue instead of dead-lettering it. **It has no
delay and no attempt limit**, so a message that always fails is a loop that consumes a
consumer for as long as the process runs. It exists for the case where the failure is
obviously transient and the queue is obviously short; the ladder is the answer the rest of the
time.

## Which attempt is this

`message.Attempt` counts from one, and `message.IsFirstAttempt` is the readable form of
`Attempt <= 1`:

```csharp
public async Task<Ack> HandleAsync(IMessage<Order> message, CancellationToken ct)
{
    if (!message.IsFirstAttempt)
    {
        _log.LogWarning(
            "order {Id}, attempt {Attempt}", message.Payload.Id, message.Attempt);
    }

    // The age of the message rather than of this attempt, which is what a decision about
    // giving up should be made on.
    if (message.Envelope.Age > TimeSpan.FromHours(1))
    {
        return Ack.DeadLetter("older than an hour");
    }

    return Ack.Accept();
}
```

The counter travels **on the message**, not in the process, which is why a restart does not
reset it and why a redelivery after a failed drain comes round at the same number. It is
also why the raw AMQP header cannot be trusted for this: a broker requeues the bytes it was
given, so the wire header still says what it said the first time. `message.Attempt` is the
library's count and is the one to read.

## Handling a message once

A retry means a redelivery, and a redelivery means the handler runs again — on a message some
of whose work may already have been done. If the handler only reads, that is free. If it
charges a card, it is not.

Two answers, and the first is better when it is available.

### Make the work idempotent

An `INSERT ... ON CONFLICT DO NOTHING` keyed on the message id, an update that sets a state
rather than incrementing a counter, a payment provider's own idempotency key — none of these
need anything from this package, and all of them are correct across instances and restarts,
which is more than a store can promise.

### Or ask for a store

`r.Idempotency` on a consumer registration hands the library a store, and the library claims
the message before the handler runs:

```csharp
builder.Services.AddSingleton<IIdempotencyStore>(_ =>
    new InMemoryIdempotencyStore(TimeSpan.FromHours(6)));

builder.Services
    .AddAceMq(builder.Configuration.GetSection("acemq"))
    .AddConsumer<Order, OrderHandler>("orders.new", configure: r =>
        r.Idempotency = sp => sp.GetRequiredService<IIdempotencyStore>());
```

It is a **factory**, `Func<IServiceProvider, IIdempotencyStore>`, and not a store, because
`AddConsumer` runs while the container is still being built — so a store that is a service in
it cannot be an argument here. The factory is called once, when that consumer starts. A store
you already have is `r.Idempotency = _ => store;`.

**There is no configuration key for this and there will not be one.** A store worth having is
a connection string, a table name and a retention window at minimum, and naming one in
`appsettings.json` would mean this package opening database connections.

### What the store sees

The key is **`message.Envelope.Id`** — the library's own message id, which travels with the
message and survives a republish. Not the AMQP message id, and not anything the handler
chooses.

The sequence is worth knowing because it explains the interface:

1. `ClaimAsync(id)` before the handler runs. **False means the message is accepted and the
   handler is never called** — silently, because from the application's point of view it was
   handled, just not now.
2. The handler runs.
3. `ConfirmAsync(id)` when it succeeded, so a later redelivery is refused at step one.
4. `ReleaseAsync(id)` when it failed, so the **retry is not mistaken for a duplicate**. This
   is the step a hand-rolled deduplication gets wrong, and getting it wrong means the first
   failure is the last attempt.

Claiming before rather than after is also what stops two consumers handling the same message
concurrently, which is a real case at concurrency above one.

### The stores in the box

`InMemoryIdempotencyStore(retention)` — and `InMemoryIdempotencyStore.ForOneDay()` — is
bounded by age and by size, `100_000` entries unless a second argument says otherwise. Read
its own documentation before relying on it: **it is per process and lost on restart.** It
deduplicates the redeliveries one running consumer sees, which is most of them, and it cannot
deduplicate across instances or across a restart. It also has an `Evictions` counter, which
is the number to watch — an eviction is a claim forgotten early, and a forgotten claim is a
duplicate waiting to happen.

`DbIdempotencyStore` is the one that means it:

```csharp
builder.Services.AddSingleton<IIdempotencyStore>(sp =>
    new DbIdempotencyStore(
        () => new NpgsqlConnection(connectionString),
        retention: TimeSpan.FromDays(7)));
```

The first argument is a `ConnectionSupplier` — `delegate DbConnection ConnectionSupplier()` —
so any ADO.NET provider works, and `CreateTableSql()` gives you the DDL to put in a
migration. The table and parameter prefix are the third and fourth arguments for a database
whose placeholders are not `@`.

**The real reason to use it is not durability, it is the transaction.** A claim written to the
same database as the work, in the same transaction, commits or rolls back with it — which is
the only arrangement where "handled once" is actually true rather than nearly true. That needs
the handler to own the transaction rather than the library, so it is the hand-written version
of this pattern; the registration above is the convenient version, and the difference is worth
understanding before choosing.

### A duplicate is invisible

By design, and it is worth saying out loud: a refused claim returns `Ack.Accept()`, so a
duplicate produces no log line from this package, no metric of its own and no trace of having
been dropped. If you need to know how many are being deduplicated, count them in the store —
a `ClaimAsync` that returns false is the event, and it is your code.

## Testing all of it

The in-memory transport redelivers, so a ladder and a store can both be tested with no
broker:

```csharp
["acemq:url"] = "memory://" + nameof(ThisTest),
["acemq:listener:retry:enabled"] = "true",
["acemq:listener:retry:maxAttempts"] = "3",
["acemq:listener:retry:initialDelay"] = "00:00:00.100",
```

Keep the delays in milliseconds and the test runs in a second. What that cannot prove is that
the rung queues exist on a real broker, which is what the integration suite is for — this
package has a test named
`The_retry_ladder_is_declared_when_a_consumer_asks_for_it` for exactly that. See
[testing](testing.md).
