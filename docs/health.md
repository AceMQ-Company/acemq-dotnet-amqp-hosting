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

The data dictionary carries `transport`, `open`, `blocked`, `inFlight`, `consumers`, a
`blockedReason` when there is one, and a `check.{name}` entry per contributor. Those are
the facts worth having in an incident.

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

If you want an alert on it, alert on `blocked` in the data, or on the broker's own metrics.
Blocked is an operational fact about the broker, not a verdict on this instance.

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
`OrderedQueue`, which registers itself — is folded in, and the worst of them wins.

The connection's **own** report is deliberately left out of that fold. The library calls a
blocked connection degraded; taking the worst of every report would let that overrule the
careful answer above with the plain one. What the connection reports is reconstructed here
from `IsOpen`, `IsBlocked` and `BlockedReason` instead. The Go library's lifecycle guide
runs into exactly the same trap from the other direction and documents the same workaround.
