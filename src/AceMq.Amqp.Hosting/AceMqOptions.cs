/*
 * Copyright 2026 AceMQ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;

namespace AceMq.Amqp.Hosting
{
    /// <summary>
    /// Everything under the <c>acemq</c> configuration section.
    /// </summary>
    /// <remarks>
    /// <para>The names follow the library's own vocabulary rather than the transport's,
    /// because a setting called something different from the method it configures is a
    /// setting somebody has to look up twice. <c>PublisherConfirms</c> sets
    /// <see cref="ConnectionConfig.Builder.WithoutPublisherConfirms"/> when false, and the
    /// two are named the same thing on purpose.</para>
    ///
    /// <para>Nothing here has a default that costs money or safety. Confirms are on, TLS
    /// verification is on when TLS is on, and the topology is not applied unless something
    /// is declared. The one opinionated default is the URL — <c>amqp://localhost:5672</c> —
    /// so a developer with a broker in Docker needs no configuration at all.</para>
    ///
    /// <para>Durations bind through the configuration binder's <see cref="TimeSpan"/>
    /// converter, so they are written <c>"00:00:30"</c> in <c>appsettings.json</c>, not
    /// <c>30s</c>. That is a difference from the Spring starter and it is the framework's
    /// convention, not a choice made here.</para>
    /// </remarks>
    public sealed class AceMqOptions
    {
        /// <summary>The configuration section these are bound from by default.</summary>
        public const string SectionName = "acemq";

        /// <summary>
        /// Whether to configure AceMQ at all. Off, no connection is made and no consumer
        /// runs — which is what a test host, or a batch instance of the same application,
        /// wants. The services are still registered, so nothing fails to resolve.
        /// </summary>
        public bool Enabled { get; set; } = true;

        /// <summary>
        /// Broker URL. The scheme selects the transport, so <c>amqps://</c> is how TLS is
        /// asked for.
        /// </summary>
        public string Url { get; set; } = "amqp://localhost:5672";

        /// <summary>Username. Left unset, the credentials in the URL are used, if any.</summary>
        public string? Username { get; set; }

        /// <summary>Password.</summary>
        public string? Password { get; set; }

        /// <summary>Virtual host. Unset means the transport's default.</summary>
        public string? VirtualHost { get; set; }

        /// <summary>
        /// Connection name shown in the broker's management UI. Defaults to
        /// <see cref="Microsoft.Extensions.Hosting.IHostEnvironment.ApplicationName"/>,
        /// because "unnamed connection" on a page of forty is the same as no name at all.
        /// </summary>
        public string? ClientName { get; set; }

        /// <summary>How long to wait for the TCP and protocol handshake.</summary>
        public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>How long a publish waits for its confirm before it is a failure.</summary>
        public TimeSpan ConfirmTimeout { get; set; } = TimeSpan.FromSeconds(30);

        /// <summary>
        /// Publisher confirms. On; turning them off means a send that returns has not been
        /// accepted by anything, only written to a socket.
        /// </summary>
        public bool PublisherConfirms { get; set; } = true;

        /// <summary>
        /// The most unconfirmed publishes allowed at once. The back pressure that stops a
        /// producer outrunning a broker until the process runs out of memory.
        /// </summary>
        public int MaxOutstandingPublishes { get; set; } = 10_000;

        /// <summary>
        /// The codec, by its name in the library's registry: <c>json</c>, <c>bytes</c>,
        /// <c>string</c> or <c>xml</c> out of the box, plus whatever an application has
        /// registered with <c>CodecRegistry.Register</c> before the host starts.
        /// </summary>
        public string Format { get; set; } = "json";

        /// <summary>TLS.</summary>
        public AceMqTlsOptions Tls { get; set; } = new AceMqTlsOptions();

        /// <summary>Exchanges, queues and bindings to declare at startup.</summary>
        public AceMqTopologyOptions Topology { get; set; } = new AceMqTopologyOptions();

        /// <summary>Defaults for every consumer, each of which can override them.</summary>
        public AceMqListenerOptions Listener { get; set; } = new AceMqListenerOptions();
    }

    /// <summary>What TLS verification to do.</summary>
    public enum AceMqTlsMode
    {
        /// <summary>No TLS. The default; an <c>amqps://</c> URL turns it on regardless.</summary>
        Disabled,

        /// <summary>Verify the certificate chain and the hostname.</summary>
        Required,

        /// <summary>Verify neither. For a laboratory, and it reads like it.</summary>
        Insecure,
    }

    /// <summary>
    /// TLS settings.
    /// </summary>
    /// <remarks>
    /// There is no <c>VerifyHostname: false</c> setting, and there will not be one. The
    /// library expresses that as <c>TlsOptions.Insecure()</c>, whose name survives a code
    /// review; a boolean in a settings file does not, because the line that disabled
    /// verification for one afternoon reads exactly like the lines around it.
    /// </remarks>
    public sealed class AceMqTlsOptions
    {
        /// <summary>
        /// The mode. Left <see cref="AceMqTlsMode.Disabled"/> with an <c>amqps://</c> URL,
        /// the connection is still TLS and still verified — the scheme wins, and the
        /// unsafe direction is never the implicit one.
        /// </summary>
        public AceMqTlsMode Mode { get; set; } = AceMqTlsMode.Disabled;

        /// <summary>
        /// PEM or DER file holding the certificate authority to trust, for a broker whose
        /// certificate a public root does not vouch for.
        /// </summary>
        public string? CertificateAuthority { get; set; }

        /// <summary>PKCS#12 or PEM file holding the client certificate, for mutual TLS.</summary>
        public string? ClientCertificate { get; set; }

        /// <summary>Password for <see cref="ClientCertificate"/>.</summary>
        public string? ClientCertificatePassword { get; set; }

        /// <summary>
        /// The name to verify the broker's certificate against, when it differs from the
        /// host in the URL — a broker reached through a tunnel or a service mesh.
        /// </summary>
        public string? ServerName { get; set; }

        /// <summary>
        /// Check revocation. On, and worth leaving on; turning it off is for an air-gapped
        /// network where the responder is unreachable and the check only adds latency
        /// before failing open anyway.
        /// </summary>
        public bool CheckRevocation { get; set; } = true;

        /// <summary>
        /// Accept certificates carrying AceMQ's development marker. A production broker's
        /// certificate does not carry it, so this cannot silently weaken a real deployment
        /// — it only allows the ones that announce themselves as untrustworthy.
        /// </summary>
        public bool AllowDevelopmentCertificates { get; set; }
    }

    /// <summary>How much of the declared topology to actually declare.</summary>
    public enum TopologyApply
    {
        /// <summary>Declare nothing. The topology is documentation only.</summary>
        None,

        /// <summary>Declare what is missing. The default when anything is declared.</summary>
        Create,

        /// <summary>
        /// Declare nothing, but compare and log. What to run against production before
        /// running <see cref="Create"/> against it.
        /// </summary>
        DryRun,
    }

    /// <summary>The exchanges, queues and bindings an application declares at startup.</summary>
    public sealed class AceMqTopologyOptions
    {
        /// <summary>
        /// What to do with the declarations. <see cref="TopologyApply.Create"/> once
        /// anything is declared, and nothing at all when nothing is.
        /// </summary>
        public TopologyApply Apply { get; set; } = TopologyApply.Create;

        /// <summary>
        /// Refuse to start when the broker's topology differs from the declared one in a
        /// way a declaration cannot fix — a queue that exists with different arguments.
        /// Off, the drift is logged at warning and the application starts.
        /// </summary>
        public bool FailOnDrift { get; set; }

        /// <summary>Exchanges.</summary>
        public IList<ExchangeOptions> Exchanges { get; set; } = new List<ExchangeOptions>();

        /// <summary>Queues.</summary>
        public IList<QueueOptions> Queues { get; set; } = new List<QueueOptions>();

        /// <summary>Bindings.</summary>
        public IList<BindingOptions> Bindings { get; set; } = new List<BindingOptions>();

        /// <summary>True when nothing at all has been declared.</summary>
        public bool IsEmpty =>
            Exchanges.Count == 0 && Queues.Count == 0 && Bindings.Count == 0;

        /// <summary>An exchange.</summary>
        public sealed class ExchangeOptions
        {
            /// <summary>Name.</summary>
            public string Name { get; set; } = string.Empty;

            /// <summary>Type: <c>topic</c>, <c>direct</c>, <c>fanout</c> or <c>headers</c>.</summary>
            public string Type { get; set; } = "topic";
        }

        /// <summary>A queue.</summary>
        public sealed class QueueOptions
        {
            /// <summary>Name.</summary>
            public string Name { get; set; } = string.Empty;

            /// <summary>
            /// Kind. Quorum by default: a classic queue on a cluster acknowledges a
            /// message that one node has, and that node can be the one that dies.
            /// </summary>
            public QueueType Type { get; set; } = QueueType.Quorum;

            /// <summary>Declare <c>{name}.dlq</c> and point this queue's dead letters at it.</summary>
            public bool DeadLetter { get; set; }

            /// <summary>Broker arguments, passed through as declared.</summary>
            public IDictionary<string, string> Arguments { get; set; } =
                new Dictionary<string, string>();
        }

        /// <summary>A binding.</summary>
        public sealed class BindingOptions
        {
            /// <summary>The queue that receives.</summary>
            public string Queue { get; set; } = string.Empty;

            /// <summary>The exchange that routes.</summary>
            public string Exchange { get; set; } = string.Empty;

            /// <summary>The routing key, or a pattern for a topic exchange.</summary>
            public string RoutingKey { get; set; } = string.Empty;
        }
    }

    /// <summary>Defaults applied to every registered consumer.</summary>
    public sealed class AceMqListenerOptions
    {
        /// <summary>
        /// Unacknowledged messages allowed per consumer. 100 is a starting point, not an
        /// answer: the right number is a function of handler time and message size, and
        /// the only way to find it is to measure. It is also what bounds a drain — every
        /// delivery already fetched is handled before shutdown finishes.
        /// </summary>
        public int Prefetch { get; set; } = 100;

        /// <summary>Consumers per registration. More than one means messages are no longer ordered.</summary>
        public int Concurrency { get; set; } = 1;

        /// <summary>Start consumers when the host starts.</summary>
        public bool AutoStartup { get; set; } = true;

        /// <summary>
        /// How long shutdown waits for handlers still running before it stops waiting.
        /// </summary>
        /// <remarks>
        /// Must be shorter than the host's own <c>ShutdownTimeout</c>, or the host stops
        /// waiting first and the drain never gets the time it was configured for. The
        /// consumer host logs a warning at startup when it is not.
        /// </remarks>
        public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(20);

        /// <summary>Requeue a message whose handler threw, rather than dead-lettering it.</summary>
        public bool RequeueOnFailure { get; set; }

        /// <summary>The retry ladder.</summary>
        public AceMqRetryOptions Retry { get; set; } = new AceMqRetryOptions();
    }

    /// <summary>
    /// The retry ladder applied to a handler that fails.
    /// </summary>
    /// <remarks>
    /// Off by default. A retry that is on by default is a retry nobody chose, and the
    /// library's ladder republishes with a delay rather than sleeping in the handler —
    /// which is the whole point, and worth knowing you have asked for.
    /// </remarks>
    public sealed class AceMqRetryOptions
    {
        /// <summary>Enable the ladder.</summary>
        public bool Enabled { get; set; }

        /// <summary>Total attempts, the first one included.</summary>
        public int MaxAttempts { get; set; } = 3;

        /// <summary>The first delay.</summary>
        public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>The ceiling on the delay.</summary>
        public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(1);

        /// <summary>What each delay is multiplied by.</summary>
        public double Multiplier { get; set; } = 2.0;

        /// <summary>
        /// Randomness added to each delay, as a fraction. Zero means a fleet that failed
        /// together retries together.
        /// </summary>
        public double Jitter { get; set; }

        /// <summary>
        /// Give up on a message older than this, however many attempts are left. Unset,
        /// only <see cref="MaxAttempts"/> stops it.
        /// </summary>
        public TimeSpan? GiveUpAfter { get; set; }

        /// <summary>
        /// Delays at or above this wait on a rung queue in the broker rather than in this
        /// process.
        /// </summary>
        /// <remarks>
        /// A shutdown concern as much as a reliability one: a wait spent in the process
        /// holds a delivery and a prefetch slot, and shutdown has to sit through it. A
        /// wait spent on the broker holds nothing and a restart does not have to survive
        /// it. Thirty seconds is the library's own default.
        /// </remarks>
        public TimeSpan BrokerWaitThreshold { get; set; } = TimeSpan.FromSeconds(30);
    }
}
