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
    /// Turns <see cref="AceMqOptions"/> into the library's own types.
    /// </summary>
    /// <remarks>
    /// Separate from the registration and the hosted services, and public, because the
    /// mapping from configuration to a <see cref="ConnectionConfig"/> is the part most
    /// worth testing and the part an application is most likely to want to inspect. Every
    /// method here is pure: no I/O, no container, no ambient state.
    /// </remarks>
    public static class AceMqConnections
    {
        /// <summary>Builds the connection configuration.</summary>
        /// <param name="options">The bound <c>acemq</c> section.</param>
        /// <returns>The configuration the library connects with.</returns>
        public static ConnectionConfig ConfigFrom(AceMqOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            var builder = ConnectionConfig.ForUrl(options.Url)
                .ConnectionTimeout(options.ConnectionTimeout)
                .ConfirmTimeout(options.ConfirmTimeout)
                .MaxOutstandingPublishes(options.MaxOutstandingPublishes);

            if (!options.PublisherConfirms) builder.WithoutPublisherConfirms();
            if (!string.IsNullOrEmpty(options.ClientName)) builder.ClientName(options.ClientName!);
            if (!string.IsNullOrEmpty(options.VirtualHost)) builder.VirtualHost(options.VirtualHost!);

            // Only when both are set. A username with no password is more likely a typo
            // than an intention, and silently sending an empty password to a broker that
            // accepts it is the kind of thing that works in development and fails in
            // production for a reason nobody can see.
            if (!string.IsNullOrEmpty(options.Username) && options.Password != null)
            {
                builder.Credentials(options.Username!, options.Password);
            }

            var tls = TlsFrom(options);
            if (tls != null) builder.Tls(tls);

            return builder.Build();
        }

        /// <summary>
        /// Builds the TLS settings, or null when there are none to apply.
        /// </summary>
        /// <remarks>
        /// An <c>amqps://</c> URL implies <see cref="AceMqTlsMode.Required"/> even when the
        /// mode was left at its default. The scheme is the clearer statement of intent and
        /// the safe direction is the implicit one; asking for TLS and getting plaintext
        /// because a second setting was missed is not a mistake this should be capable of.
        /// </remarks>
        /// <param name="options">The bound <c>acemq</c> section.</param>
        /// <returns>The TLS settings, or null.</returns>
        public static TlsOptions? TlsFrom(AceMqOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));

            var amqps = options.Url != null &&
                        options.Url.StartsWith("amqps://", StringComparison.OrdinalIgnoreCase);
            var mode = options.Tls.Mode;
            if (mode == AceMqTlsMode.Disabled && !amqps) return null;
            if (mode == AceMqTlsMode.Disabled) mode = AceMqTlsMode.Required;

            var tls = mode == AceMqTlsMode.Insecure ? TlsOptions.Insecure() : TlsOptions.Required();

            if (!string.IsNullOrEmpty(options.Tls.CertificateAuthority))
            {
                tls = tls.TrustCertificateAuthority(options.Tls.CertificateAuthority!);
            }

            if (!string.IsNullOrEmpty(options.Tls.ClientCertificate))
            {
                tls = tls.WithClientCertificate(
                    options.Tls.ClientCertificate!, options.Tls.ClientCertificatePassword);
            }

            if (!string.IsNullOrEmpty(options.Tls.ServerName))
            {
                tls = tls.WithServerName(options.Tls.ServerName!);
            }

            if (!options.Tls.CheckRevocation) tls = tls.WithoutRevocationChecking();
            if (options.Tls.AllowDevelopmentCertificates) tls = tls.AllowDevelopmentCertificates();

            return tls;
        }

        /// <summary>Resolves the codec named by <see cref="AceMqOptions.Format"/>.</summary>
        /// <param name="options">The bound <c>acemq</c> section.</param>
        /// <returns>The codec.</returns>
        public static ICodec CodecFrom(AceMqOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            var name = string.IsNullOrEmpty(options.Format)
                ? CodecRegistry.DefaultFormat
                : options.Format;
            return CodecRegistry.ByName(name);
        }

        /// <summary>
        /// Builds the declared topology, or null when nothing is declared or the apply
        /// mode is <see cref="TopologyApply.None"/>.
        /// </summary>
        /// <param name="options">The topology section.</param>
        /// <returns>The topology, or null.</returns>
        public static Topology? TopologyFrom(AceMqTopologyOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (options.Apply == TopologyApply.None || options.IsEmpty) return null;

            var builder = Topology.Define();

            foreach (var exchange in options.Exchanges)
            {
                Require(exchange.Name, "acemq:topology:exchanges[].name");
                builder.Exchange(exchange.Name, exchange.Type);
            }

            foreach (var queue in options.Queues)
            {
                Require(queue.Name, "acemq:topology:queues[].name");
                // The configuration binder gives strings; the broker takes objects. An
                // argument that looks like a number is passed as one, because x-max-length
                // as the string "1000" is rejected by RabbitMQ with an error that names
                // the type and not the setting.
                IReadOnlyDictionary<string, object>? arguments = null;
                if (queue.Arguments.Count > 0)
                {
                    var map = new Dictionary<string, object>();
                    foreach (var entry in queue.Arguments) map[entry.Key] = Coerce(entry.Value);
                    arguments = map;
                }

                if (queue.DeadLetter)
                {
                    builder.QueueWithDeadLetter(queue.Name, queue.Type, arguments!);
                }
                else
                {
                    builder.Queue(queue.Name, queue.Type, arguments!);
                }
            }

            foreach (var binding in options.Bindings)
            {
                Require(binding.Queue, "acemq:topology:bindings[].queue");
                Require(binding.Exchange, "acemq:topology:bindings[].exchange");
                builder.Bind(binding.Queue, binding.Exchange, binding.RoutingKey ?? string.Empty);
            }

            return builder.Build();
        }

        /// <summary>
        /// Builds the retry policy, or null when retries are off.
        /// </summary>
        /// <param name="options">The retry section.</param>
        /// <returns>The policy, or null.</returns>
        public static RetryPolicy? RetryPolicyFrom(AceMqRetryOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            if (!options.Enabled) return null;

            var policy = RetryPolicy.Exponential(
                options.MaxAttempts, options.InitialDelay, options.Multiplier, options.MaxDelay);

            if (options.Jitter > 0) policy = policy.WithJitter(options.Jitter);
            if (options.GiveUpAfter.HasValue) policy = policy.GiveUpAfter(options.GiveUpAfter.Value);
            policy = policy.WaitInBrokerFrom(options.BrokerWaitThreshold);

            return policy;
        }

        /// <summary>
        /// Builds the consumer options for one registration, folding the listener defaults
        /// under whatever that registration overrides.
        /// </summary>
        /// <param name="defaults">The listener defaults.</param>
        /// <param name="registration">The registration, or null for the defaults alone.</param>
        /// <param name="codec">The codec the connection was opened with.</param>
        /// <returns>The consumer options.</returns>
        public static ConsumerOptions ConsumerOptionsFrom(
            AceMqListenerOptions defaults,
            AceMqConsumerRegistration? registration,
            ICodec? codec)
        {
            if (defaults == null) throw new ArgumentNullException(nameof(defaults));

            var options = ConsumerOptions.Prefetch(registration?.Prefetch ?? defaults.Prefetch);
            if (codec != null) options = options.As(codec);

            if (registration?.RequeueOnFailure ?? defaults.RequeueOnFailure)
            {
                options = options.RequeueingOnFailure();
            }

            var policy = RetryPolicyFrom(registration?.Retry ?? defaults.Retry);
            if (policy != null) options = options.WithRetry(policy);

            return options;
        }

        private static void Require(string? value, string setting)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException($"{setting} is required and was not set", setting);
            }
        }

        private static object Coerce(string value)
        {
            if (long.TryParse(value, System.Globalization.NumberStyles.Integer,
                    System.Globalization.CultureInfo.InvariantCulture, out var number))
            {
                return number;
            }

            if (bool.TryParse(value, out var flag)) return flag;
            return value;
        }
    }
}
