# Startup and shutdown

## The order

Two `IHostedService`s are registered, and their order is the whole design:

1. **`AceMqConnectionHost`** opens the connection and applies the topology.
2. **`AceMqConsumerHost`** starts the consumers.

The generic host starts hosted services in registration order and stops them in reverse, so
the connection is open and the topology is declared before any consumer starts, and the
consumers have drained before anything else is torn down. The connection itself is closed
last of all: it is owned by `IAceMqConnectionProvider`, a singleton, and the container
disposes its singletons after every hosted service has stopped.

Starting a consumer while the services it calls are still being constructed is how a
message arrives at a half-built application, and it happens on exactly the deployments
where the queue already has a backlog. That is why consumers start from a hosted service
and not from a constructor.

## Getting the connection

```csharp
public sealed class Orders(AceMqConnection mq) { }          // blocks on first resolution
public sealed class Orders(IAceMqConnectionProvider p) { }  // awaits
```

Connecting is asynchronous and constructing a service is not. `AceMqConnection` is
registered directly for the code that would rather inject it, and its factory blocks on the
provider. That is safe wherever a modern host runs — there is no synchronization context to
deadlock against — but it does block, so a constructor that resolves it before the host has
started pays for the connection there. In the ordinary case `AceMqConnectionHost` has
already opened it and the factory hands back an open connection immediately.

`IAceMqConnectionProvider.GetAsync()` is the async-correct path, and what a
`BackgroundService` should use.

## What a drain finishes, precisely

`AceMqConsumerHost.StopAsync` does three things, in this order, and the order is not
interchangeable.

**First it stops handing messages to handlers.** A delivery the broker has already sent is
held rather than rejected, so the consumer keeps its place in the queue and resumes with the
same message rather than cycling it to the back.

**Then it waits for the handlers already running.** Each finishes, and its decision is
carried out — the message is acknowledged, or retried, or dead-lettered, exactly as it
would have been.

**Only then, and only if that ran out of time, does it cancel the token the handlers were
given.**

| at the moment of shutdown | what happens |
|---|---|
| a handler is running | it runs to completion, and its decision is carried out |
| a delivery has arrived but no handler has it yet | it is held unacknowledged, and the broker redelivers it after the process goes |
| a publish is waiting for its confirm | nothing waits for it; it is cut off |
| a retry is waiting out a backoff in this process | the drain waits with it, for the whole delay |

Two of those rows deserve more than a line.

### The publish waiting for a confirm

Nothing waits for it. A `SendAsync` in flight when shutdown begins is not part of the drain,
and a connection closing under it will fail it. If a publish must survive a restart it
belongs in the library's outbox, not in a shutdown hook — see the library's
[patterns](https://acemq.org/acemq-dotnet-amqp/patterns.html) page.

This is also why a `BackgroundService` that publishes should stop before the consumers do,
which the generic host gives you for free if it is registered after `AddAceMq`.

### The retry waiting out a backoff

A short wait is spent in this process, holding the delivery; a long one is spent on a rung
queue in the broker, holding nothing. Which is which is `listener:retry:brokerWaitThreshold`,
thirty seconds by default.

At shutdown that difference stops being about prefetch slots and becomes about whether the
drain finishes at all, because the drain waits out an in-process backoff in full. A consumer
two seconds into a two-second wait makes the drain take the rest of it. A consumer on a
five-minute schedule that fell back to waiting here — because its rung queue was missing —
makes the drain take five minutes, which is to say it makes the drain fail, because nothing
grants a pod five minutes.

So the rung queues are a shutdown concern and not only a reliability one. Lower the
threshold and the waits move to the broker:

```json
"listener": { "retry": { "enabled": true, "brokerWaitThreshold": "00:00:05" } }
```

`AddConsumer` declares the rungs of the policy it is given, so a consumer normally finds
them.

### The delivery that never reached a handler

This one differs from the Go library and is worth naming. Go's `Close` drains the delivery
channel: every message the transport had already handed over is handled in full, so the work
a drain has to get through is bounded by **prefetch**. Here it is not. A delivery that has
arrived but has not reached a handler is held at the pause gate, and it is not counted as
in flight — so the drain reports itself finished with those deliveries still held. When the
connection closes they were never acknowledged, so the broker gives them to somebody else,
or to this process after it restarts.

The practical consequences, both of them real:

- **A drain here is bounded by `concurrency`, not by `prefetch`.** It finishes faster, and
  a large prefetch does not make shutdown slow.
- **The redelivery burst after it is larger.** Up to `prefetch` messages per consumer come
  back, where the Go library would have handled them. Nothing is lost, and nothing is
  handled twice unless a handler had already started — but a queue depth graph will show
  the step.

"Drained cleanly" therefore means *every handler finished*, which is what the log line
says. It does not mean *every message the broker had sent was handled*. If that stronger
guarantee is what you need, keep prefetch close to concurrency.

How many that was is now a number rather than an inference. `AceMq.Amqp` 0.7.0 added
`AceMqConnection.Held`, the count of deliveries fetched and waiting at the pause gate, and
this package reads it: it appears as `held` in the [health data](health.md) and in the
drain's own log lines, so a shutdown says how many messages it handed back as well as that
it finished.

```
drained in 00:00:00.8140000; every handler finished. 7 delivery(ies) were fetched but
never handled — they are unacknowledged and the broker will redeliver them.
```

A shutdown with no held deliveries logs the shorter line, unchanged.

## The two deadlines

`listener:shutdownTimeout` is what the drain is given. `HostOptions.ShutdownTimeout` —
thirty seconds unless changed — is when the host stops waiting for hosted services at all.
**The smaller one wins, and it is the host's.**

A drain configured for sixty seconds under a host that waits thirty gets thirty. The host's
token is handed to the drain itself, so the two endings stay distinguishable and each has
its own log line: a drain that was given long enough and did not finish says so against its
budget, and a drain the host stopped waiting for says *that* instead, naming both times.

```
the host stopped waiting after 00:00:30, before the 00:01:00 the drain was given: 3
handler(s) still running and 0 delivery(ies) held. ... Raise HostOptions.ShutdownTimeout,
or lower acemq:listener:shutdownTimeout below it.
```

`AceMqConsumerHost` also warns at startup when the two are set that way round, before any
message has been taken:

```
acemq:listener:shutdownTimeout is 00:01:00 and the host's ShutdownTimeout is 00:00:30.
The host stops waiting first, so the drain never gets the time it was given.
```

Set the drain shorter than the host's, and the host's shorter than whatever your
orchestrator grants before `SIGKILL`. A Kubernetes `terminationGracePeriodSeconds` of 60,
a host `ShutdownTimeout` of 45 and a drain of 20 is a configuration that behaves.

## Cancellation is last, never first

Cancelling the handlers' token is not a way to hurry a drain along. It is what to do once
the drain has already failed, so the deliveries go back to the broker unsettled rather than
being abandoned against a connection that is about to close. They come round again after
the restart at the same attempt number, because the attempt counter travels on the message
rather than in the process. One extra attempt, no lost message.

## Readiness

The health check reports **unhealthy from the moment shutdown begins**, so an instance that
has been told to stop leaves the load balancer's rotation before it starts refusing work.
See [health checks](health.md), and note in particular that it should not be wired to a
liveness probe.
