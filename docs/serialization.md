# Serialization and codecs

What goes on the wire. One setting for the common case, and one hosting-specific rule for
everything else — the rule is [where to register](#registration-happens-before-the-host-starts),
and it is the whole reason this page exists rather than a link to the library's.

## The setting

```json
{ "acemq": { "format": "json" } }
```

| `format` | Content type | |
|---|---|---|
| `json` | `application/json` | The default. `System.Text.Json`, camel-cased, case-insensitive on read. |
| `bytes` | `application/octet-stream` | No serialisation. A `byte[]` payload straight through. |
| `string` | `text/plain; charset=utf-8` | UTF-8 text. |
| `xml` | `application/xml` | For a consumer that cannot be changed. |

The name is looked up in the library's `CodecRegistry`, so **anything registered there is a
valid value** — including a codec of your own, an encrypting one, a claim check or Avro. That
is the extension point, and it is a string in a settings file rather than a type, which is
what lets an encrypted codec be turned on per environment.

## Registration happens before the host starts

`CodecRegistry` is **static and global**, and the codec named by `format` is resolved when
the connection opens — inside `AceMqConnectionHost.StartAsync`. So a codec of your own has to
be registered before that, and the honest place is `Program.cs`, before `Build()`:

```csharp
using AceMq.Amqp;

// Before the host is built. Not in a hosted service, not in AddAceMq's callback, and not
// in a static constructor nothing forces to run.
CodecRegistry.Register("orders-v3", () => new JsonCodec(new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
}));

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddAceMq(builder.Configuration.GetSection("acemq"));
await builder.Build().RunAsync();
```

```json
{ "acemq": { "format": "orders-v3" } }
```

An unregistered name fails the connection with an `AceFatalException` naming the format, which
is the right failure and arrives at startup rather than at the first message.

Three consequences of "static and global" that are easy to meet and hard to diagnose:

- **It is not scoped to a host.** Two hosts in one process share one registry, which matters
  in a test suite more than anywhere else: register once in a fixture, not per test, and
  expect a second `Register` of the same name to be the one that wins.
- **It cannot take a dependency from the container.** A codec that needs a key ring, a blob
  store or a schema registry gets it from a closure — constructed in `Program.cs` where the
  configuration is readable but the container is not built. If it needs a service, build the
  thing it needs by hand there.
- **`Register` takes a factory**, `Func<ICodec>`, not an instance, so the codec is constructed
  when it is resolved. For a codec holding a connection pool that is the difference between
  one and one per resolution — which in practice is one, because `AceMqConnections.CodecFrom`
  resolves once per connection.

`CodecRegistry.Register(ICodecProvider)` is the other overload, for a codec that would rather
carry its own name than be handed one:

```csharp
public sealed class OrdersCodec : ICodecProvider
{
    public string Name => "orders-v3";
    public ICodec Create() => new JsonCodec(Options);
}

CodecRegistry.Register(new OrdersCodec());
```

`CodecRegistry.Names()` lists what is registered, which is worth logging once at startup in an
application that registers several.

## One codec per connection

**There is no per-consumer codec.** `ConsumerOptions.As(codec)` exists in the library, and
this package does not reach it: `AceMqConsumerHost` builds each consumer's options with
`codec: null`, which means every consumer decodes with the connection's codec. `AddConsumer`
has no parameter for it and `AceMqConsumerRegistration` has no property for it.

So an application consuming JSON on one queue and Protobuf on another has three options, and
the first is nearly always right:

1. **A `CompositeCodec`**, which chooses on content type. [Below](#reading-two-formats-at-once).
2. **`format: "bytes"`** and decode in the handler, which is honest when the two formats have
   nothing in common.
3. **Two connections**, which means not using `AddAceMq` for one of them.

This is a genuine gap against what the library can do and is listed as one.

## Writing a codec

Four members, and the fourth is the one that matters:

```csharp
public sealed class CsvCodec : ICodec
{
    public string ContentType => "text/csv";

    public byte[] Encode(object payload) =>
        Encoding.UTF8.GetBytes(Render((Reading)payload));

    public object Decode(byte[] body, Type target) =>
        Parse(Encoding.UTF8.GetString(body));

    // What this codec is willing to read. Consulted by CompositeCodec and by the
    // library when a message arrives carrying a content type — so a codec that
    // answers true to everything is a codec that will be handed a body it cannot
    // parse.
    public bool CanDecode(string? contentType) =>
        contentType != null &&
        contentType.StartsWith("text/csv", StringComparison.OrdinalIgnoreCase);
}
```

`Decode` receives the target `Type` — the `TMessage` of the `AddConsumer` that is about to be
called — so a codec can deserialise into the type the handler asked for rather than into a
dictionary. `CodecExtensions.Decode<T>(codec, body)` is the generic convenience over it, for
code calling a codec directly.

`IContentTypeCodec` is the second, optional interface: implement it and `Decode` is handed the
message's content type as well as the body. Anything wrapping another codec wants it —
`CompositeCodec` and `ClaimCheckCodec` both implement it.

## Reading two formats at once

The migration case: a queue being read while producers are moving from one format to the
other. `CompositeCodec` encodes with the **first** codec and decodes with whichever one says
it `CanDecode` the message's content type:

```csharp
CodecRegistry.Register("json-then-xml", () =>
    CompositeCodec.Of(new JsonCodec(), new XmlCodec()));
```

```json
{ "acemq": { "format": "json-then-xml" } }
```

New messages go out as JSON; messages still on the queue as XML are read. Once the queue has
drained of XML, the format goes back to `json` and the composite is deleted — which is the
shape of the whole manoeuvre, and the reason `format` being a string in a settings file is
worth more than it looks.

Order matters twice. The first codec is the one that encodes, and the codecs are asked in
order whether they can decode — so put the specific one before the permissive one.
`BytesCodec.CanDecode` returns **true for everything**, so a composite with `bytes` anywhere
but last will never reach what comes after it.

## Schema evolution

A producer on a new schema and a consumer on an old one, which is every deployment where the
two are not released together. JSON tolerates this by accident: an unknown field is ignored on
read and a missing one is a default. That is usually enough and occasionally catastrophic,
because nothing tells you a field was silently dropped.

Avro makes it explicit, and `AceMq.Amqp.Avro` is a separate package:

```bash
dotnet add package AceMq.Amqp.Avro --version 0.7.2
```

```csharp
// The producer's schema and the consumer's are different, and that is the point: the
// reader schema is the shape this application understands. Avro resolves between them,
// so a field added by a producer with a default is readable here and a field this reader
// expects is filled from its default when the writer did not send it.
var registry = new DbSchemaRegistry(() => new NpgsqlConnection(connectionString));

CodecRegistry.Register("orders-avro", () =>
    AvroCodec.Registered(registry, writerSchemaJson, readerSchemaJson));
```

`AvroCodec.Registered` puts a schema id on the wire and the schema itself in the registry, so
a consumer can resolve a writer schema it has never seen. `AvroCodec.Of(schemaJson)` is the
fixed-schema form with no registry — content type `avro/binary` rather than
`application/vnd.acemq.avro` — and is for a pair that really do move together.
`WithoutReaderSchema()` reads with the writer's schema as given, which is what a generic
consumer wants and what a typed one does not.

`ISchemaRegistry` is two methods, `IdFor(SchemaDefinition)` and `SchemaFor(int)`.
`InMemorySchemaRegistry` is for tests; `DbSchemaRegistry` takes the same
`ConnectionSupplier` as the other database-backed stores and has a `CreateTableSql()` for a
migration.

**The registry is built in `Program.cs`, not resolved from the container**, for the reason in
the section above — and this is the case where that pinches, because a registry wants the same
connection string as everything else. Read it from `builder.Configuration` before `Build()`;
it is available there.

A schema change that is not backward compatible is not a serialisation problem and no codec
will save it. The answer is a new message type on a new routing key, with both consumed until
the old one stops arriving.

## Claim check

A body too large for a queue. `ClaimCheckCodec` wraps another codec and, **above a
threshold**, puts the body in a store and sends the key instead:

```csharp
CodecRegistry.Register("claim-checked", () =>
    ClaimCheckCodec.Wrapping(
        new JsonCodec(),
        new FilesystemClaimCheckStore("/var/lib/acemq/claims"),
        threshold: 256 * 1024));
```

Below the threshold the message is an ordinary message — that is what makes this safe to
leave on, rather than a decision per publish. `ClaimCheckCodec.DefaultThreshold` is 64 KB if
you do not pass one.

Both ends need the same store and the same registration, and **the consumer needs read access
to it**, which is the deployment fact this pattern lives or dies by. A store the producer can
write and the consumer cannot reach turns every large message into a decode failure.

`IClaimCheckStore` is three synchronous methods — `Put`, `Get`, `Delete`. `Put` and `Get` are
the two that are called for you; **`Delete` never is**, so nothing collects the bodies and the
store grows for ever. That is deliberate — a codec cannot know when the last consumer of a
message has finished with it — and it means a claim-check store needs a retention policy of
its own, whether that is an S3 lifecycle rule or a nightly job.

`FilesystemClaimCheckStore(directory)` is for one machine or a shared volume;
`InMemoryClaimCheckStore` is for tests and is not a claim check, since the body never leaves
the process that published it. A real deployment implements the interface over object storage,
and it is three methods.

`ClaimCheckCodec.IsClaimCheck(body)` and `.KeyOf(body)` inspect a body without fetching it,
which is what to reach for when a consumer reports a key its store does not have.

## Encryption

Same mechanism — a codec that wraps another — and it composes with the two above, because a
claim check wrapping an encrypted codec stores ciphertext. It is on the
[security page](security.md#payload-encryption), with the key-rotation part that matters.

## The other formats

Each in its own package, so an application carries only the one it uses:

| Package | Codec | Content type |
|---|---|---|
| `AceMq.Amqp.Avro` | `AvroCodec` | `avro/binary` or `application/vnd.acemq.avro` |
| `AceMq.Amqp.Protobuf` | `ProtobufCodec` | `application/x-protobuf` |
| `AceMq.Amqp.Yaml` | `YamlCodec` | `application/yaml` |
| `AceMq.Amqp.Toml` | `TomlCodec` | `application/toml` |
| `AceMq.Amqp.Xml` | `InteropXmlCodec` | `application/xml` |
| `AceMq.Amqp.Crypto` | `EncryptedCodec` | `application/vnd.acemq.encrypted` |

None of them is registered by default. Each is `CodecRegistry.Register("name", () => new
...())` in `Program.cs` and then a string in `appsettings.json`.

`InteropXmlCodec` is a second XML codec and not a replacement for the built-in `xml`: it
exists for XML that has to be read by something not written in .NET, and it takes a root
element name. If both ends are .NET, the built-in is simpler.

## What travels beside the body

The codec encodes the **payload**. Everything else about a message — its type, correlation id,
attempt count, first-seen timestamp, trace context and any application header — is the
`Envelope`, and the envelope is not encoded by the codec. It goes on the wire as AMQP headers.

Two things follow. The envelope is readable whatever the codec does, which is how the library
routes and retries a message whose body it cannot decode — and it is why
[encryption does not protect headers](security.md#headers-are-not-encrypted).

## Testing a codec

A codec is a pure function of bytes, so the test worth having needs nothing:

```csharp
[Fact]
public void It_round_trips()
{
    var codec = new CsvCodec();
    var encoded = codec.Encode(new Reading("t-1", 21.5));
    Assert.Equal(new Reading("t-1", 21.5), codec.Decode<Reading>(encoded));
}
```

The one that catches real bugs is the other direction: decode a body produced by the *other*
language's library and assert on the result. Cross-language content-type agreement is the part
that goes wrong, and `CanDecode` is where it goes wrong.

For the wiring — that `format` reaches the connection at all — the in-memory transport is
enough, and a registration in a fixture rather than in a test, per the warning
[above](#registration-happens-before-the-host-starts). See [testing](testing.md).
