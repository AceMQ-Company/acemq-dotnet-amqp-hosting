# Reporting a vulnerability

Email **security@acemq.com** with what you found and how to reproduce it. Please do
not open a public issue for anything exploitable.

You should get an acknowledgement within two working days, and an assessment of
whether it is a vulnerability, what is affected, and a rough timeline within a week.
If a fix is warranted, we will tell you when it is released and credit you unless you
would rather we did not.

## What is in scope

Everything in this repository: `AceMq.Amqp.Hosting` and
`AceMq.Amqp.Hosting.OpenTelemetry`.

Things worth reporting even if they feel minor:

- Configuration that asks for TLS and produces a connection without it, or with
  verification weaker than it asked for. In particular: an `amqps://` URL that does
  not verify, or a `tls:mode` that is quietly downgraded.
- A development certificate accepted without `tls:allowDevelopmentCertificates`.
- Anything that renders a credential, a key, or a message body into a log or an
  exception message — including the connection URL's password, which is redacted and
  must stay redacted.
- Anything that makes the health check's data dictionary carry a secret. It is served
  to whatever the application maps it to.
- A handler scope that outlives its message, or is shared between two messages
  handled at once. Two messages sharing a scoped service is a data-leak shape as much
  as a correctness one.

## What is not

- **The health check reporting a blocked broker as healthy.** That is deliberate and
  documented — see [health checks](docs/health.md). A report that an application
  stays in rotation while its broker applies back pressure is a report about a
  decision, not a defect.
- **`tls:mode: Insecure` accepting any certificate.** That is what it is for, and it
  says so in its name rather than in a comment.
- **Vulnerabilities in AceMq.Amqp itself** — report those against
  [acemq-dotnet-amqp](https://github.com/AceMQ-Company/acemq-dotnet-amqp), which is
  where a fix would land.
- **Vulnerabilities in RabbitMQ, Microsoft.Extensions or OpenTelemetry** — report
  those to Broadcom, Microsoft and the OpenTelemetry project respectively.
- Findings from a scanner with no demonstrated impact.

## Supported versions

Pre-1.0, only the latest release. There are no maintenance branches yet, so a fix
means a new patch version.

## What this package does not do for you

It configures a connection and runs consumers. It does not manage broker users or
permissions, hold your keys, authenticate any endpoint it registers a check for, or
decide what your health endpoints are exposed to. The library's
[security guide](https://acemq.org/acemq-dotnet-amqp/security.html) covers the
connection; the rest is your application's.
