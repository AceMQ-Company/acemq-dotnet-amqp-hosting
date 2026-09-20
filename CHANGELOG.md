# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the versions follow
[semantic versioning](https://semver.org/spec/v2.0.0.html).

This package has its own version line, starting at 0.1.0, because it tracks the
Microsoft.Extensions release train as much as it tracks AceMQ's. A change in either can
force a release here, and a version number shared with the library could only ever say that
one of the two had moved.

## [Unreleased]

## [0.1.0] - 2026-09-20

The first release. Dependency injection and hosted-service integration for AceMQ AMQP:
one `AddAceMq` call, handlers resolved from the container, a consumer host that drains on
shutdown, and a health check.

**Built against the published `AceMq.Amqp` 0.7.0**, from
<https://acemq.org/nuget/index.json>. The nuspec asks for 0.7.0 or newer, so an
application already on a later library keeps it.

### Added

- `AddAceMq` on `IServiceCollection`: binds the `acemq` configuration section, registers an
  `IAceMqConnectionProvider` and an `AceMqConnection`, two hosted services and a health
  check, and returns a builder for registering consumers. Idempotent — a second call
  configures rather than duplicating.
- `AceMqOptions`: everything under `acemq` — URL, credentials, virtual host, client name,
  timeouts, publisher confirms, `maxOutstandingPublishes`, codec, TLS, topology, and
  listener defaults including a retry ladder. Validated when first read, with the setting
  named in the failure.
- `IMessageHandler<T>` and `AddConsumer<TMessage, THandler>`: a handler type resolved from
  the container, from a fresh scope per message. `AddConsumer<TMessage>(queue, delegate)`
  for the three-line case. A typed interface rather than an attribute discovered by
  scanning, because constructor injection, compile-time checking and a scope per message all
  come free with a registration and none of them come free with an attribute.
- `AceMqConnectionHost`: opens the connection and applies the declared topology before any
  consumer starts. Reports drift, and refuses to start on it when
  `acemq:topology:failOnDrift` is set.
- `AceMqConsumerHost`: starts the consumers after the host is built and drains them when it
  stops. Handlers in flight run to completion and their decisions are carried out; the token
  they were given is cancelled only after the drain has overrun, so the deliveries go back
  to the broker rather than being abandoned against a closing connection. Honours the host's
  own `ShutdownTimeout` as well as `acemq:listener:shutdownTimeout`, and warns at startup
  when the two are set so that the host would stop waiting first.

  The drain is bounded by the host's own token, through
  `DrainConsumersAsync(TimeSpan, CancellationToken)`, where cancelling abandons the wait and
  not the work: the handlers are not interrupted and consuming stays paused. The two ways a
  drain can end are different log lines, which is the point — a drain that was given long
  enough and did not finish, and a drain the host stopped waiting for. The second names both
  deadlines and says which to change, because that one is a misconfiguration rather than a
  slow handler.

  A finished drain says how many deliveries it handed back. "Every handler finished" — which
  is all a drain ever guarantees — is logged alongside `AceMqConnection.Held`, the number of
  messages fetched and never handled, rather than leaving the reader to infer it from a
  queue-depth graph after the fact.
- `AceMqHealthCheck`, registered as `acemq` and tagged `ready`. A blocked connection is
  reported **healthy with the reason**, matching the Spring Boot starter: an application
  that fails its own health check for back pressure is one an orchestrator restarts into the
  same blocked broker. Draining is reported unhealthy, which is the opposite decision for
  the opposite reason.

  Its `Data` is the library's connection report, passed through rather than rebuilt, **so
  every value in it is a string**:

  ```json
  { "open": "true", "blocked": "false", "transport": "rabbitmq",
    "inFlight": "0", "held": "0", "consumers": "1" }
  ```

  Worth knowing before writing an alert rule against it: `HealthReport.Details` in the
  library is an `IReadOnlyDictionary<string, string>`, so a rule of the form
  `data.blocked == true` never fires — match `"true"`. `consumers` is this package's own
  count and is rendered the same way deliberately, so a reader of the dictionary needs one
  rule rather than two. `held` counts deliveries fetched and waiting at the pause gate;
  `blockedReason` appears only when the broker gave one; `consuming` and `publishing`
  appear with the value `"paused"` only while they are; `check.{name}` appears per
  registered contributor.

  The blocked description — `the broker has blocked this connection:` followed by the
  broker's reason — is the sentence the Go, Python and Ruby libraries also use, so a single
  alert rule reads a blocked broker in any of them. .NET's framework hands back a
  structured dictionary alongside the description, so here the fact lives in `blocked` and
  the sentence is for the human reading a dashboard.
- `AceMq.Amqp.Hosting.OpenTelemetry`: `AddAceMqInstrumentation()` on `TracerProviderBuilder`
  and `MeterProviderBuilder`. A separate package so that an application without
  OpenTelemetry does not carry it; it depends on `OpenTelemetry.Api` only, not the SDK.
- `AceMqTelemetryNames`, for an application wiring the `ActivitySource` and `Meter` by hand.
- `examples/worker`: a runnable worker with configuration, a handler type, a publisher,
  OpenTelemetry and a drain. Built and run against a broker in CI, because an example that
  has stopped working is worse than none.
- Documentation at <https://acemq.org/acemq-dotnet-amqp-hosting/> — eleven pages, including
  a lifecycle page that is specific about what a drain does *not* finish.

### Notes

- **Targets `netstandard2.0` and `net8.0`**, matching `acemq-dotnet-amqp`. netstandard2.0
  survives because the Microsoft.Extensions packages this needs still carry that asset on
  the 8.0 line, which is why the dependency is pinned there rather than to the newest.
  CI checks the built assembly and the packed `lib/` folders rather than the project file.
- **Depends on the released `AceMq.Amqp` 0.7.0** from <https://acemq.org/nuget/index.json>,
  not a project reference, so the repository proves the published package works from an
  application. CI checks the produced nuspec for that dependency.
- `AceMq.Amqp.HealthStatus` and `Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus`
  share a name, and inside a namespace under `AceMq.Amqp` — which this package's is — the
  enclosing namespace beats a file-level `using HealthStatus = …`. Both are aliased
  explicitly here and in the tests. 0.7.0 documents the collision on the library's own
  enum, so it is no longer folklore.
- `AceMq.Amqp.RabbitMq` ships with the package. Transports in this library are registered by
  hand rather than discovered by scanning, so a package that configures a connection from a
  URL and carries no transport would fail at the first `amqp://`.
- 52 unit tests over the library's in-memory transport, and 6 integration tests against
  RabbitMQ 4. The drain is covered in both.

### Known gaps

- No `appsettings.json` IntelliSense. The Spring Boot starter ships
  `spring-configuration-metadata.json`; there is no equivalent here yet. XML documentation
  gives completion on the `AddAceMq(o => ...)` callback, not in the JSON file.
- A drain is bounded by `concurrency`, not by `prefetch`: a delivery the broker has sent but
  no handler has taken is held and redelivered rather than handled, which differs from the
  Go library and is documented on the lifecycle page. It is a visible difference rather than
  a silent one — `AceMqConnection.Held` counts them, and the drain log line and the health
  data both report it.
- `acemq:blockedTimeout` exists in the Spring starter and not here, because the .NET
  library's `ConnectionConfig` has no equivalent to set.

How a release is cut is in [RELEASING.md](RELEASING.md).

[Unreleased]: https://github.com/AceMQ-Company/acemq-dotnet-amqp-hosting/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/AceMQ-Company/acemq-dotnet-amqp-hosting/releases/tag/v0.1.0
