# Configuration

Everything under the `acemq` section, bound to `AceMqOptions` through the ordinary options
pipeline. Anything that can be set here can also be set in code:

```csharp
builder.Services.AddAceMq(
    builder.Configuration.GetSection("acemq"),
    o => o.Listener.Prefetch = 20);
```

The callback runs after the configuration is bound, so it wins.

`AddAceMq()` with no section binds `acemq` from whatever `IConfiguration` the container
has. `AddAceMq(configuration.GetSection("messaging"))` binds a section of your choosing.

## Durations

`TimeSpan` values go through the configuration binder's own converter, so they are written
`"00:00:30"`, not `30s`. That is the framework's convention rather than a choice made here,
and it is a difference from the Spring Boot starter worth knowing before copying a
`application.yaml` across.

## The connection

| Setting | Default | What it does |
|---|---|---|
| `enabled` | `true` | Off, nothing connects and no consumer runs. The services are still registered, so nothing fails to resolve — which is what a test host, or a batch instance of the same application, wants. |
| `url` | `amqp://localhost:5672` | The broker. The **scheme selects the transport**, so `amqps://` is how TLS is asked for. |
| `username` | — | Unset, the credentials in the URL are used. Set without a password it is ignored, deliberately: half-set credentials are more often a typo than an intention. |
| `password` | — | |
| `virtualHost` | — | Unset means the transport's default. |
| `clientName` | the application name | What the broker's management UI shows. "unnamed connection" on a page of forty is the same as no name at all. |
| `connectionTimeout` | `00:00:10` | The TCP and protocol handshake. |
| `confirmTimeout` | `00:00:30` | How long a publish waits for its confirm before it is a failure. |
| `publisherConfirms` | `true` | Off, a `SendAsync` that returns has been written to a socket and accepted by nothing. |
| `maxOutstandingPublishes` | `10000` | The back pressure that stops a producer outrunning a broker until the process runs out of memory. |
| `format` | `json` | The codec, by name: `json`, `bytes`, `string`, `xml`, plus anything registered with `CodecRegistry.Register` before the host starts. |

## TLS

| Setting | Default | What it does |
|---|---|---|
| `tls:mode` | `Disabled` | `Required` verifies the certificate chain and the hostname. `Insecure` verifies neither. |
| `tls:certificateAuthority` | — | A PEM or DER file holding the authority to trust, for a broker no public root vouches for. |
| `tls:clientCertificate` | — | PKCS#12 or PEM, for mutual TLS. |
| `tls:clientCertificatePassword` | — | |
| `tls:serverName` | — | The name to verify against, when it differs from the host in the URL — a broker reached through a tunnel or a mesh. |
| `tls:checkRevocation` | `true` | |
| `tls:allowDevelopmentCertificates` | `false` | Accepts certificates carrying AceMQ's development marker. A production certificate does not carry it, so this cannot silently weaken a real deployment. |

**An `amqps://` URL turns TLS on even when `mode` is left at `Disabled`**, and turns it on
as `Required`. The scheme is the clearer statement of intent, and the safe direction is the
implicit one: asking for TLS and getting plaintext because a second setting was missed is
not a mistake this should be capable of.

There is no `verifyHostname: false`, and there will not be one. The library expresses that
as `Insecure`, whose name survives a code review; a boolean in a settings file does not,
because the line that disabled verification for one afternoon reads exactly like the lines
around it.

## Listener defaults

Every consumer takes these unless it overrides them.

| Setting | Default | What it does |
|---|---|---|
| `listener:prefetch` | `100` | Unacknowledged messages allowed per consumer. A starting point, not an answer — and see [startup and shutdown](lifecycle.md), because it is also what bounds a drain. |
| `listener:concurrency` | `1` | Consumers per registration. More than one means messages are no longer ordered. |
| `listener:autoStartup` | `true` | Start consumers when the host starts. |
| `listener:shutdownTimeout` | `00:00:20` | How long shutdown waits for handlers still running. **Must be shorter than the host's own `ShutdownTimeout`** or the host stops waiting first; a warning is logged at startup when it is not. |
| `listener:requeueOnFailure` | `false` | Requeue a message whose handler threw, rather than dead-lettering it. |

## The retry ladder

Off by default. A retry that is on by default is a retry nobody chose, and this ladder
republishes with a delay rather than sleeping in the handler — which is the whole point,
and worth knowing you have asked for.

| Setting | Default | What it does |
|---|---|---|
| `listener:retry:enabled` | `false` | |
| `listener:retry:maxAttempts` | `3` | Total attempts, the first one included. |
| `listener:retry:initialDelay` | `00:00:01` | |
| `listener:retry:maxDelay` | `00:01:00` | |
| `listener:retry:multiplier` | `2.0` | Below 1 is refused: it would make each retry sooner than the last. |
| `listener:retry:jitter` | `0` | A fraction. Zero means a fleet that failed together retries together. |
| `listener:retry:giveUpAfter` | — | Give up on a message older than this, however many attempts are left. |
| `listener:retry:brokerWaitThreshold` | `00:00:30` | Delays at or above this wait on a rung queue in the broker rather than in this process. |

`brokerWaitThreshold` is a **shutdown** setting as much as a reliability one. A wait spent
in the process holds a delivery and a prefetch slot, and shutdown has to sit through it; a
wait spent on the broker holds nothing and a restart does not have to survive it. Lowering
it is the cheapest way to make a drain predictable.

## Topology

See [topology](topology.md) for the shape and what each field means.

| Setting | Default |
|---|---|
| `topology:apply` | `Create` — and nothing at all when nothing is declared |
| `topology:failOnDrift` | `false` |
| `topology:exchanges` | `[]` |
| `topology:queues` | `[]` |
| `topology:bindings` | `[]` |

## Validation

Configuration that cannot work is refused when the options are first read, with the setting
named. A prefetch of zero, a multiplier below one, a URL without a scheme, a max delay
shorter than the initial delay, a queue with no name: each of these otherwise fails several
layers down in terms that do not mention the setting that caused it.

Nothing is validated when `enabled` is false.

## No IDE completion for `appsettings.json`

The Spring Boot starter ships `spring-configuration-metadata.json` and properties complete
as you type them. There is no equivalent here yet. The XML documentation on `AceMqOptions`
gives full IntelliSense on the `AddAceMq(o => ...)` callback, which is the path that does
have tooling; the JSON file does not. This is a real gap against the starter and it is
listed as such.
