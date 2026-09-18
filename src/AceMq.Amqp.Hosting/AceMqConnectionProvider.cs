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
using System.Threading;
using System.Threading.Tasks;
using AceMq.Amqp.RabbitMq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AceMq.Amqp.Hosting
{
    /// <summary>
    /// Hands out the one connection, opening it on first use.
    /// </summary>
    /// <remarks>
    /// Connecting is asynchronous and constructing a service is not, which is the whole
    /// reason this exists. Anything that can wait asks here; <see cref="AceMqConnection"/>
    /// is also registered directly for the code that would rather inject it, and that
    /// registration blocks on this one.
    /// </remarks>
    public interface IAceMqConnectionProvider
    {
        /// <summary>
        /// The connection if it is already open, and null if it is not. For a health
        /// check or a log line that must not itself cause a connection.
        /// </summary>
        AceMqConnection? Current { get; }

        /// <summary>
        /// The connection, opening it if it is not open yet.
        /// </summary>
        /// <param name="cancellationToken">Cancels the wait, not the connect.</param>
        /// <returns>The connection.</returns>
        Task<AceMqConnection> GetAsync(CancellationToken cancellationToken = default);
    }

    /// <summary>The default <see cref="IAceMqConnectionProvider"/>.</summary>
    internal sealed class AceMqConnectionProvider : IAceMqConnectionProvider, IDisposable
    {
        private readonly IOptions<AceMqOptions> _options;
        private readonly ILogger<AceMqConnectionProvider> _log;
        private readonly SemaphoreSlim _gate = new SemaphoreSlim(1, 1);

        private AceMqConnection? _connection;
        private bool _disposed;

        public AceMqConnectionProvider(
            IOptions<AceMqOptions> options, ILogger<AceMqConnectionProvider> log)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _log = log ?? throw new ArgumentNullException(nameof(log));
        }

        public AceMqConnection? Current => _connection;

        public async Task<AceMqConnection> GetAsync(CancellationToken cancellationToken = default)
        {
            var existing = _connection;
            if (existing != null) return existing;

            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                if (_disposed) throw new ObjectDisposedException(nameof(AceMqConnectionProvider));
                if (_connection != null) return _connection;

                var options = _options.Value;
                if (!options.Enabled)
                {
                    throw new InvalidOperationException(
                        "AceMQ is disabled (acemq:enabled is false), so there is no connection to " +
                        "hand out. Resolve IAceMqConnectionProvider only from code that runs when " +
                        "it is enabled, or turn it on.");
                }

                // Registered here rather than in AddAceMq, because a transport registered
                // at service-registration time is registered even for a host that is
                // configured never to connect. Transports.Register is keyed by scheme and
                // idempotent, so calling it once per connect costs nothing.
                Transports.Register(new RabbitMqTransport());

                var config = AceMqConnections.ConfigFrom(options);
                var codec = AceMqConnections.CodecFrom(options);

                _log.LogInformation(
                    "connecting to {Config} with the {Codec} codec", config, codec.ContentType);

                _connection = await AceMqConnection
                    .ConnectAsync(config, codec, cancellationToken)
                    .ConfigureAwait(false);

                _log.LogInformation(
                    "connected over {Transport}, capabilities: {Capabilities}",
                    _connection.TransportName,
                    string.Join(", ", _connection.Capabilities));

                return _connection;
            }
            finally
            {
                _gate.Release();
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            // Disposed by the container when the host is torn down, which is after every
            // IHostedService has stopped — so after the consumer host has drained. That
            // ordering is the point: disposing a connection with handlers still running
            // abandons their work.
            _connection?.Dispose();
            _connection = null;
            _gate.Dispose();
        }
    }
}
