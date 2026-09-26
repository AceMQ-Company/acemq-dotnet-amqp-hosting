# Health checks

`AddAceMq` registers one `IHealthCheck`, named `acemq`, tagged `acemq` and `ready`. There
is nothing to add.

```csharp
app.MapHealthChecks("/readyz", new HealthCheckOptions
{
    Predicate = r => r.Tags.Contains("ready"),
});
```

## What it reports

| | status | why |
|---|---|---|
| connected, nothing wrong | Healthy | |
| **connected, broker has blocked the connection** | **Healthy**, with the reason | see below |
| a registered contributor is degraded | Degraded | an ordered queue with a halted partition, say |
| a registered contributor is down | Unhealthy | |
| the connection is not open | Unhealthy | |
| not connected yet, or a connect that failed | Unhealthy | |
| shutting down | Unhealthy, `"draining"` | |
| `acemq:enabled` is false | Healthy | nothing was asked of it |

The data dictionary carries `transport`, `open`, `blocked`, `inFlight`, `held`,
`consumers`, a `blockedReason` when there is one, `consuming` and `publishing` when either
is paused, and a `check.{name}` entry per contributor. Those are the facts worth having in
an incident.

**Every value is a string.** `open` is `"true"`, not `true`; `inFlight` is `"0"`, not `0`.
Most of them come straight out of the library's own `connection.Health()` report, and that
report is a `IReadOnlyDictionary<string, string>`; `consumers` is this package's own count
and is rendered the same way so that a reader needs one rule rather than two. A rule
written against a boolean or a number silently stops matching — see
[the changelog](https://github.com/AceMQ-Company/acemq-dotnet-amqp-hosting/blob/main/CHANGELOG.md)
if you wrote one before `AceMq.Amqp` 0.7.0.

`held` is new in `AceMq.Amqp` 0.7.0 and is worth an eye during a shutdown: it counts
deliveries the broker has already sent that are waiting at the pause gate, fetched and not
handled. A finished drain means every *handler* finished, not that `held` was zero. See
[the lifecycle page](lifecycle.md).

## A blocked connection is not a reason to restart

RabbitMQ sends `connection.blocked` when it is low on memory or disk, and stops reading
from the connection until the pressure clears. An application that fails its own health
check for that is an application an orchestrator restarts — into the same blocked broker,
having thrown away whatever it was holding. A fleet doing that in unison is a queue that
stops being drained at the moment it most needs draining.

So a blocked connection is **Healthy, with the reason in the description and in the data**.
The Spring Boot starter reports it the same way, for the same reason.

`HealthStatus.Degraded` was the obvious alternative and was rejected. It maps to 200 in the
framework's own defaults, so it would have been harmless there — but
`HealthCheckOptions.ResultStatusCodes` is routinely changed to map Degraded to 503, and a
choice whose safety depends on a setting in somebody else's file is not a choice.

If you want an alert on it, alert on `blocked == "true"` in the data, or on the broker's
own metrics. Blocked is an operational fact about the broker, not a verdict on this
instance.

The data key is the contract here, not the sentence. The Go, Python and Ruby libraries all
write one identical sentence for a blocked connection so that a single alert rule reads a
blocked broker whatever a service is written in; .NET's framework gives a structured
dictionary alongside the description, so the dictionary is where the fact lives and
`blocked` is the key to match. The description — `the broker has blocked this connection:`
followed by the broker's reason — is this package's own wording and is meant for a human
reading a dashboard.

## Draining is unhealthy, and that is the opposite decision

Once shutdown begins the check reports Unhealthy with the description `draining`. That is
the right answer to "should traffic still come here", which is the question a readiness
probe is asking. An instance that has been told to stop should leave the rotation before it
starts refusing work.

## Do not wire this to a liveness probe

Liveness is "this process has stopped being a process" — deadlocked, out of file
descriptors, a leak that ate the heap. Nothing about the broker belongs there. A process
that cannot reach its broker is not a process that restarting fixes; restarting throws away
every message it was holding and reconnects to the same unreachable broker.

```csharp
// Liveness: this process is running. Nothing about the broker.
app.MapHealthChecks("/livez", new HealthCheckOptions { Predicate = _ => false });

// Readiness: this process can do the work.
app.MapHealthChecks("/readyz", new HealthCheckOptions
{
    Predicate = r => r.Tags.Contains("ready"),
});
```

## Excluding it

The `acemq` tag exists for that:

```csharp
Predicate = r => r.Tags.Contains("ready") && !r.Tags.Contains("acemq")
```

## Contributors

Anything registered with the library — `connection.RegisterHealth(contributor)`, or an
`OrderedQueue`, which registers itself — is folded in, and the worst of them wins. Each
appears as `check.{name}` in the data, with `Up`, `Degraded` or `Down` for a value.

`IHealthContributor` is two members, `Name` and `Report()`, and `Report()` is **synchronous** —
which is the awkward part, because most things worth reporting on are not. Worked examples of
contributors for the things this package does not know about: an
[outbox backlog](outbox.md#health-and-metrics) and a
[stopped stream reader](streams.md#failure-and-skipfailures).

Note that `AceMq.Amqp.HealthStatus` is not
`Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus`. A contributor returns the
library's; inside a namespace under `AceMq.Amqp` the enclosing namespace beats a file-level
`using` alias, so write it out or declare the alias inside the namespace.

The connection's own report is folded in with them, and its details are copied into the
data as they are.

That is only true from `AceMq.Amqp` 0.7.0. Up to 0.6.0 the library reported a blocked
connection as `Degraded`, and since the worst report wins, folding the connection's own
report in would have let that one reading overrule the careful answer above with a plain
one. So this package left the connection's report out of the fold and rebuilt the facts
from `IsOpen`, `IsBlocked` and `BlockedReason` instead. 0.7.0 reports a blocked connection
as `Up`, with `blocked` and `blockedReason` among its details, and the workaround went with
the reason for it.
