# Licence and warranty

AceMQ hosting for .NET is [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
You may use it in production, commercially, without asking and without paying.

## No warranty

The licence disclaims warranties and limits liability — sections 7 and 8. In plain terms:
this is provided as it is, and if it loses your messages that is your risk to have taken.

That is not a formality to skim. This package is at 0.1.0, it sits on a pre-1.0 library, and
its documentation says which parts have been proven against a real broker and which have
not. [Startup and shutdown](lifecycle.md) is the page to read before deciding how much to
rely on the drain, and it is deliberately specific about what a drain does *not* finish.

If you need somebody accountable for it working, that is what
[Enterprise support](https://acemq.com) is for. The package is complete and free without
it, and is not crippled to sell it.

## What you must do

Keep the licence and the copyright notice with any copy or derivative, and state what you
changed. That is the whole obligation.

## Dependencies

`AceMq.Amqp.Hosting`:

| | |
|---|---|
| `AceMq.Amqp` | Apache-2.0, AceMQ |
| `AceMq.Amqp.RabbitMq` | Apache-2.0, AceMQ — and through it `RabbitMQ.Client`, Apache-2.0 or MPL-2.0, Broadcom |
| `Microsoft.Extensions.Hosting` | MIT, Microsoft |
| `Microsoft.Extensions.DependencyInjection.Abstractions` | MIT, Microsoft |
| `Microsoft.Extensions.Options.ConfigurationExtensions` | MIT, Microsoft |
| `Microsoft.Extensions.Logging` | MIT, Microsoft |
| `Microsoft.Extensions.Diagnostics.HealthChecks` | MIT, Microsoft |

`AceMq.Amqp.Hosting.OpenTelemetry` adds `OpenTelemetry.Api` — Apache-2.0, the OpenTelemetry
Authors. Not the SDK: your application keeps its own choice of that.

## Trademarks

RabbitMQ is a trademark of Broadcom Inc. and/or its subsidiaries. .NET is a trademark of
Microsoft. OpenTelemetry is a trademark of The Linux Foundation. AceMQ is an independent
project, affiliated with and endorsed by none of them.
