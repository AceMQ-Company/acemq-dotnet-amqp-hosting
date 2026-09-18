# Examples

## `worker`

One runnable program with everything this package does turned on: a connection and a
topology from `appsettings.json`, a handler type resolved from the container per message, a
publisher on a timer to give it work, OpenTelemetry on both signals, a health check, and a
drain on shutdown.

```bash
docker run -d --name acemq-example -p 5672:5672 rabbitmq:4-alpine
dotnet run --project examples/worker
```

Then stop it with Ctrl-C and read the last few lines:

```
info: Microsoft.Hosting.Lifetime[0]
      Application is shutting down...
info: AceMq.Amqp.Hosting.AceMqConsumerHost[0]
      draining 1 consumer(s); 2 message(s) in flight, 00:00:20 to finish
info: AceMq.Amqp.Hosting.AceMqConsumerHost[0]
      drained in 00:00:01.8413363; every handler finished
```

The handler sleeps for two seconds on purpose, so there is always something for the drain
to wait for.

Point it somewhere else with an environment variable — the configuration binder's separator
is a double underscore:

```bash
acemq__url=amqp://guest:guest@localhost:5721 dotnet run --project examples/worker
```

It is built **and run** in CI, against a real broker, with a `SIGTERM` and an assertion that
`every handler finished` appears in the log. An example that has stopped compiling is worse
than no example; one that has stopped working is worse still.
