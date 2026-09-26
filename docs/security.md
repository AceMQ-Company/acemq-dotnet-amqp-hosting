# Security

What this package can be told about transport security, credentials and payload
confidentiality — and, as clearly, what it cannot.

Three of the four are configuration. TLS is `acemq:tls`, credentials are `acemq:url` or
`acemq:username`/`acemq:password`, and development certificates are one flag. Payload
encryption is not: it is a codec, and a codec is a key ring, which does not belong in a
settings file.

## TLS

```json
{
  "acemq": {
    "url": "amqps://broker.internal:5671",
    "tls": {
      "mode": "Required",
      "certificateAuthority": "/etc/acemq/ca.pem",
      "clientCertificate": "/etc/acemq/client.pfx",
      "clientCertificatePassword": "...",
      "serverName": "broker.internal",
      "checkRevocation": true
    }
  }
}
```

| Setting | Default | What it does |
|---|---|---|
| `tls:mode` | `Disabled` | `Required` verifies the certificate chain **and** the hostname. `Insecure` verifies neither. |
| `tls:certificateAuthority` | — | A PEM or DER file holding the authority to trust, for a broker no public root vouches for. Which is most internal brokers. |
| `tls:clientCertificate` | — | PKCS#12 or PEM, for mutual TLS. |
| `tls:clientCertificatePassword` | — | The password for it. |
| `tls:serverName` | — | The name to verify the broker's certificate against, when it differs from the host in the URL. |
| `tls:checkRevocation` | `true` | Whether to check revocation. |
| `tls:allowDevelopmentCertificates` | `false` | [Below](#development-certificates). |

### `amqps://` wins

**An `amqps://` URL turns TLS on even when `mode` is left at `Disabled`, and turns it on as
`Required`.** The scheme is the clearer statement of intent, and the safe direction is the
implicit one: asking for TLS and getting plaintext because a second setting was missed is
not a mistake this should be capable of.

The reverse is not symmetrical, and that is also deliberate. `mode: Required` with an
`amqp://` URL is TLS on a plaintext port, which fails at the handshake — loudly, which is
correct. Nothing here quietly downgrades.

### There is no `verifyHostname: false`

And there will not be one. The library expresses that as `Insecure`, whose name survives a
code review; a boolean in a settings file does not, because the line that disabled
verification for one afternoon reads exactly like the lines around it, and the afternoon
becomes eighteen months.

`Insecure` verifies neither the chain nor the name. It is for a laboratory and it reads like
it — which is the point.

### `serverName`, and when it is needed

A broker reached through a tunnel, a service mesh sidecar, or a load balancer presents a
certificate for its own name while the URL names the thing in front of it. Hostname
verification fails, correctly, and `serverName` is how to say which name to expect rather
than turning verification off:

```json
{ "acemq": { "url": "amqps://localhost:5671",
             "tls": { "serverName": "broker.internal" } } }
```

That is `Insecure`'s only legitimate competitor and it is strictly better: the chain is
still verified, and the name still has to match *something* you named.

### Revocation

On, and worth leaving on. Turning it off is for an air-gapped network where the responder is
unreachable, and where the check therefore adds latency before failing open anyway. If a
connection is slow to establish and the broker's certificate carries an OCSP or CRL
endpoint your network cannot reach, that is the first thing to test.

### What TLS does not cover

TLS protects the message **in transit, to the broker and no further**. The broker sees
plaintext, so its disk, its backups and its management interface do too. If that is the
threat, [payload encryption](#payload-encryption) is the answer and TLS is not.

## Credentials

Two ways, and the first is not worse:

```json
{ "acemq": { "url": "amqp://orders:s3cret@broker.internal:5672" } }
```

```json
{ "acemq": { "url": "amqp://broker.internal:5672",
             "username": "orders", "password": "s3cret" } }
```

`username` and `password` win over anything in the URL. **They are only used when both are
set**: a username with no password is more likely a typo than an intention, and silently
sending an empty password to a broker that accepts it is the kind of thing that works in
development and fails in production for a reason nobody can see.

### Keeping them out of `appsettings.json`

Nothing here needs a mechanism of its own, because `AceMqOptions` is bound through the
ordinary configuration pipeline — so **every .NET configuration provider already works**,
and the one that fits your deployment is the one to use:

```bash
# Environment variables. The double underscore is the section separator, and the
# casing does not matter.
export acemq__password='s3cret'
export acemq__url='amqp://broker.internal:5672'
```

```bash
# Development, on a machine, never in the repository.
dotnet user-secrets set "acemq:password" "s3cret"
```

```csharp
// Key Vault, Secrets Manager, a mounted file — all of them are providers, and all of
// them are added before AddAceMq reads anything.
builder.Configuration.AddAzureKeyVault(vaultUri, credential);
builder.Services.AddAceMq(builder.Configuration.GetSection("acemq"));
```

A Kubernetes secret mounted as a file is `builder.Configuration.AddKeyPerFile(...)`, and a
file named `acemq__password` lands in the right place with no code at all.

The options are bound **when they are first read**, which is when the connection host starts
— not when `AddAceMq` is called. In a `HostApplicationBuilder` the configuration is live, so
a provider added to `builder.Configuration` anywhere before `Build()` is read whether it was
added before or after `AddAceMq`. `AddAceMq()` with no section goes further and resolves
`IConfiguration` from the container at that point, which is why the no-argument overload
works in a minimal host where the sources are added last.

### Credentials are read once

This is the honest limitation and it is worth stating plainly. `AceMqOptions` is bound once
and the connection is opened once, from those values. **A password that rotates while the
process is running does not reach the connection**, and neither
`IOptionsMonitor<AceMqOptions>` nor a configuration reload changes that — nothing here
reconnects on a configuration change.

The library has an interface for exactly this, `ICredentialsProvider`, with
`CredentialsProviders.FromEnvironment(...)` and `FromFile(path)` in the box, and
`ConnectionConfig.Builder.Credentials(ICredentialsProvider)` to hand it over. **It is not
reachable from this package**: `AceMqOptions` has no setting for it, and
`AceMqConnections.ConfigFrom` — which is what builds the connection — only calls the
`Credentials(username, password)` overload.

So an application that needs credentials re-read on every connection attempt opens the
connection itself, with `ConnectionConfig` directly, and does without `AddAceMq`:

```csharp
// Instead of AddAceMq. The topology, the consumers, the health check and the drain all
// become this application's business, which is the real cost of the escape hatch.
builder.Services.AddSingleton(sp =>
{
    var config = ConnectionConfig.ForUrl("amqps://broker.internal:5671")
        .Credentials(CredentialsProviders.FromFile("/var/run/secrets/acemq"))
        .Tls(TlsOptions.Required().TrustCertificateAuthority("/etc/acemq/ca.pem"))
        .ClientName("orders")
        .Build();

    Transports.Register(new RabbitMqTransport());
    return AceMqConnection.ConnectAsync(config, new JsonCodec(), default)
        .GetAwaiter().GetResult();
});
```

`Transports.Register` is the line `AddAceMq` was doing for you, and forgetting it is a
failure at the first `amqps://` with "no transport registered for scheme".

Before doing that, check whether the deployment really needs it. A rotated secret usually
arrives with a restarted pod, and a restart re-reads everything — which is the
simplest correct answer and costs nothing. `ICredentialsProvider` earns its keep when the
credential is short-lived enough that the connection outlives it, which in practice means a
vault issuing minutes-long passwords.

It is a genuine gap against the library and is listed as one in the
[changelog](https://github.com/AceMQ-Company/acemq-dotnet-amqp-hosting/blob/main/CHANGELOG.md).

### The URL is logged

With the password replaced. That is the library's `ConnectionConfig.ToString`, not something
this package does, and it is worth knowing it is there rather than discovering it is not:

```
connecting to ConnectionConfig[amqp://orders:***@broker.internal:5672, ...] with the json codec
```

The connection name that reaches the broker's management UI is
`acemq:clientName`, defaulting to the application name. It carries no secret and is meant to
be read by whoever is looking at forty connections wondering which is which.

## Development certificates

A TLS broker on a laptop needs certificates, and certificates generated for a laptop must
never work anywhere else. The library's answer is a **marker**: everything the generator
writes carries the string

```
ACEMQ DEVELOPMENT ONLY - DO NOT TRUST
```

and the library refuses such a certificate unless the application has explicitly opted in.
A production broker's certificate does not carry the marker, so the opt-in **cannot silently
weaken a real deployment** — it only permits the certificates that announce themselves as
untrustworthy. That is what makes it safe to leave in a development `appsettings` file:

```json
// appsettings.Development.json
{
  "acemq": {
    "url": "amqps://localhost:5671",
    "tls": {
      "certificateAuthority": "certs/ca.crt",
      "allowDevelopmentCertificates": true
    }
  }
}
```

Without the flag the connection fails with a `SecurityConfigurationException` naming the
marker, which is a better failure than a chain error nobody can read.

### Generating them

The generator is a .NET global tool in the library's own repository, `AceMq.Amqp.DevCerts`.
It is on the AceMQ feed, and `dotnet tool install` cannot read that feed directly — so the
package is fetched first and installed from the current directory:

```bash
curl -O https://acemq.org/nuget/v3/flatcontainer/acemq.amqp.devcerts/0.7.2/acemq.amqp.devcerts.0.7.2.nupkg
dotnet tool install --global AceMq.Amqp.DevCerts --version 0.7.2 --source .
acemq-certs --out certs --broker localhost
```

`--source .` rather than `--add-source .`: the two are not synonyms, and the one that adds
puts the configured sources back in scope, which fails the same way.

| Flag | Default | |
|---|---|---|
| `--out <dir>` | `certs` | where they are written |
| `--broker <host>` | `localhost` | the name the server certificate is issued for |
| `--days <n>` | `30` | how long they are valid |
| `--password <p>` | `acemq-dev` | protects `client.pfx` |
| `--broker-certs <d>` | `/certs` | the path the generated `rabbitmq.conf` points at |
| `--no-broker-config` | | do not write `rabbitmq.conf` |

It writes a certificate authority, a server certificate and key, a client `.pfx` for mutual
TLS, and — unless told not to — a `rabbitmq.conf` that points a broker at them. Which makes
a TLS broker in Docker a compose service and nothing more:

```yaml
services:
  broker-tls:
    image: rabbitmq:4-alpine
    environment:
      # RabbitMQ writes its Erlang cookie under HOME, and the image default is not
      # writable on every Docker setup. Pointing it at /tmp is the difference between a
      # broker that starts and one that dies with an "eacces" nobody can interpret.
      HOME: /tmp
    ports:
      - "5671:5671"
    volumes:
      - ./certs:/certs:ro
      - ./certs/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
```

```bash
chmod 644 certs/server.key
```

The `chmod` is because the generator writes keys `0600`, which is right for a key and wrong
for a container running as another user. RabbitMQ reports an unreadable key as a listener
that failed to start, which is a long way from what it is.

The certificates expire in thirty days by default, and that is on purpose: a development
certificate that lasts a year is a development certificate that ends up somewhere it
shouldn't.

### Mutual TLS

The same two settings whether the certificate came from the generator or from a real
authority:

```json
{
  "acemq": {
    "url": "amqps://broker.internal:5671",
    "tls": {
      "clientCertificate": "/etc/acemq/client.pfx",
      "clientCertificatePassword": "acemq-dev"
    }
  }
}
```

The broker has to be configured to ask for one — `ssl_options.verify = verify_peer` and
`ssl_options.fail_if_no_peer_cert = true` — and RabbitMQ's `rabbitmq_auth_mechanism_ssl`
plugin is what turns the certificate into a user, if you want the certificate to *be* the
authentication rather than accompany it.

## Payload encryption

For when the broker itself is not trusted with the contents. `EncryptedCodec` wraps another
codec, so the body on the wire is ciphertext and everything else about the message is
unchanged:

```csharp
using AceMq.Amqp.Crypto;

// Before the host starts, because the codec is resolved by name when the connection
// opens. See serialization for why this has to be Program.cs and not a service.
CodecRegistry.Register("encrypted-json", () =>
    EncryptedCodec.Wrapping(new JsonCodec(), Keyring.Of(EncryptionKey.Generate("k1"))));

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddAceMq(builder.Configuration.GetSection("acemq"));
```

```json
{ "acemq": { "format": "encrypted-json" } }
```

`EncryptionKey.Generate` is for a test. A real key comes from wherever the application's
other secrets come from, as 32 bytes:

```csharp
var key = new EncryptionKey("2026-01", Convert.FromBase64String(secret));
CodecRegistry.Register("encrypted-json", () =>
    EncryptedCodec.Wrapping(new JsonCodec(), Keyring.Of(key)));
```

### Rotating a key

The key id travels with the message, so a key ring holds the current key for writing and
every old key for reading:

```csharp
var keyring = Keyring.Builder()
    .Add(new EncryptionKey("2025-07", Convert.FromBase64String(previous)))
    .Current(new EncryptionKey("2026-01", Convert.FromBase64String(current)))
    .Build();
```

Everything published from then on uses `2026-01`; anything still on the queue from before
still decrypts. Retire the old key when the oldest message that could be holding it is
older than the longest retention on any queue it reached — which is a number worth writing
down rather than estimating.

`EncryptedCodec.KeyIdOf(body)` reads the key id off a body without decrypting it, which is
what to reach for when a consumer reports a key it does not have.

### Headers are not encrypted

**The envelope is plaintext**, and it has to be: it is how the library routes, retries,
correlates and traces. Message type, correlation id, attempt count, first-seen timestamp and
any header an application set are all visible to the broker.

So nothing identifying belongs in a header. A customer id in `x-customer` defeats the
encryption for anyone reading the management UI, however good the body's cipher is.

### It is not free

Every message is encrypted on publish and decrypted on consume, in process. The cost is
small and it is not zero, and it is paid on a per-message basis — which matters at the rates
where it matters. Measure before assuming, and encrypt the queues that need it rather than
all of them: `format` is global to a connection, so "some queues encrypted" means either a
`CompositeCodec` that can read both or a second connection, and the first is usually
simpler. See [serialization](serialization.md#reading-two-formats-at-once).

## Large payloads, in passing

A message too big for a queue is a security-adjacent problem often met at the same time:
`ClaimCheckCodec` puts the body in a store and sends the key, above a threshold. Same
mechanism as encryption — a codec registered before the host starts — and the two compose,
because a claim check wrapping an encrypted codec stores ciphertext. See
[serialization](serialization.md#claim-check).

## What this package does not do

- **No authorisation.** Who may publish to which exchange is the broker's users, virtual
  hosts and permissions, and none of it is configured from here.
- **No secret redaction beyond the URL.** A password put into a header, or logged by a
  handler, is logged.
- **No certificate reloading.** A certificate file replaced on disk is read again on the next
  connection, which is the next process.
- **No `acemq:tls:verifyHostname`.** [Deliberately](#there-is-no-verifyhostname-false).
- **No reachable `ICredentialsProvider`.** [Above](#credentials-are-read-once).

## Reporting something

Security issues go to the address in
[SECURITY.md](https://github.com/AceMQ-Company/acemq-dotnet-amqp-hosting/blob/main/SECURITY.md),
not to the issue tracker.
