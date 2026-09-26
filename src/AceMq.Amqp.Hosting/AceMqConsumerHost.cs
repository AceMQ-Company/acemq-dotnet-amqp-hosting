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
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AceMq.Amqp.Hosting
{
    /// <summary>
    /// Starts the registered consumers after the host is built, and drains them when it
    /// stops.
    /// </summary>
    /// <remarks>
    /// <para>An <see cref="IHostedService"/> rather than a <c>BackgroundService</c>. A
    /// <c>BackgroundService</c> is a loop, and this is not one: the consumers are the
    /// library's, running on their own threads, and there is nothing for an
    /// <c>ExecuteAsync</c> to do but sleep until cancellation — which is a pattern that
    /// looks like work and is not.</para>
    ///
    /// <para>The start is deliberately late. Starting a consumer while the services it
    /// calls are still being constructed is how a message arrives at a half-built
    /// application, and it happens on exactly the deployments where the queue already has a
    /// backlog.</para>
    /// </remarks>
    public sealed class AceMqConsumerHost : IHostedService
    {
        private readonly IAceMqConnectionProvider _connections;
        private readonly IServiceProvider _services;
        private readonly IOptions<AceMqOptions> _options;
        private readonly IOptions<HostOptions> _hostOptions;
        private readonly IReadOnlyList<AceMqConsumerRegistration> _registrations;
        private readonly ILogger<AceMqConsumerHost> _log;

        private readonly Dictionary<string, IDisposable> _running =
            new Dictionary<string, IDisposable>(StringComparer.Ordinal);

        /// <summary>
        /// Cancelled only when a drain has already overrun. Handlers see this token, not
        /// the host's stopping token, so a graceful shutdown lets them finish.
        /// </summary>
        private readonly CancellationTokenSource _handlers = new CancellationTokenSource();

        private volatile bool _draining;

        /// <summary>Constructs the host. Resolved from the container.</summary>
        public AceMqConsumerHost(
            IAceMqConnectionProvider connections,
            IServiceProvider services,
            IOptions<AceMqOptions> options,
            IOptions<HostOptions> hostOptions,
            IEnumerable<AceMqConsumerRegistration> registrations,
            ILogger<AceMqConsumerHost> log)
        {
            _connections = connections ?? throw new ArgumentNullException(nameof(connections));
            _services = services ?? throw new ArgumentNullException(nameof(services));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _hostOptions = hostOptions ?? throw new ArgumentNullException(nameof(hostOptions));
            _registrations = (registrations ?? throw new ArgumentNullException(nameof(registrations)))
                .ToList();
            _log = log ?? throw new ArgumentNullException(nameof(log));
        }

        /// <summary>Every registered consumer, started or not.</summary>
        public IReadOnlyList<AceMqConsumerRegistration> Registrations => _registrations;

        /// <summary>The consumers currently running, by name.</summary>
        public IReadOnlyCollection<string> Running
        {
            get { lock (_running) return new ReadOnlyCollection<string>(_running.Keys.ToList()); }
        }

        /// <summary>
        /// True from the moment shutdown begins. The health check reads it, so an instance
        /// that has been told to stop leaves the rotation before it starts refusing work.
        /// </summary>
        public bool IsDraining => _draining;

        /// <summary>
        /// The token handed to handlers. Uncancelled for the whole of a graceful drain.
        /// </summary>
        public CancellationToken HandlerToken => _handlers.Token;

        /// <inheritdoc />
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var options = _options.Value;
            if (!options.Enabled) return;

            if (_registrations.Count == 0)
            {
                _log.LogDebug("no consumers registered");
                return;
            }

            WarnIfTheHostWillNotWaitLongEnough(options.Listener);

            var duplicates = _registrations
                .GroupBy(r => r.Name, StringComparer.Ordinal)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .ToList();
            if (duplicates.Count > 0)
            {
                throw new InvalidOperationException(
                    "two consumers share the name '" + duplicates[0] +
                    "'; give one of them an explicit name with AddConsumer(..., name: ...)");
            }

            var connection = await _connections.GetAsync(cancellationToken).ConfigureAwait(false);

            foreach (var registration in _registrations)
            {
                if (!(registration.AutoStartup ?? options.Listener.AutoStartup))
                {
                    _log.LogDebug(
                        "consumer {Name} not started: auto-startup is off", registration.Name);
                    continue;
                }

                await StartAsync(connection, registration).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Starts one consumer that was registered with auto-startup off.
        /// </summary>
        /// <param name="name">The consumer's name.</param>
        /// <returns>A task that completes when it is consuming.</returns>
        public async Task StartAsync(string name)
        {
            lock (_running)
            {
                if (_running.ContainsKey(name)) return;
            }

            var registration = _registrations.FirstOrDefault(r => r.Name == name);
            if (registration == null)
            {
                throw new ArgumentException("no consumer named '" + name + "'", nameof(name));
            }

            var connection = await _connections.GetAsync().ConfigureAwait(false);
            await StartAsync(connection, registration).ConfigureAwait(false);
        }

        private async Task StartAsync(
            AceMqConnection connection, AceMqConsumerRegistration registration)
        {
            var options = _options.Value;
            var consumerOptions = AceMqConnections.ConsumerOptionsFrom(
                options.Listener, registration, codec: null);

            // Applied here rather than in ConsumerOptionsFrom, which is pure and takes no
            // container. A store is resolved once per consumer at the moment it starts,
            // which is after the application's services are built and is the earliest point
            // at which resolving one is correct.
            if (registration.Idempotency != null)
            {
                consumerOptions = consumerOptions.Idempotent(registration.Idempotency(_services));
            }

            var concurrency = registration.Concurrency ?? options.Listener.Concurrency;

            IDisposable handle;
            try
            {
                handle = await registration
                    .Start(connection, _services, consumerOptions, concurrency, _handlers.Token)
                    .ConfigureAwait(false);
            }
            catch (Exception e) when (!(e is OperationCanceledException))
            {
                // The transport's own message for a missing queue is a broker close-reason
                // with a 404 in it, and nothing in it says which consumer asked or where
                // the queue was supposed to come from. Both of those are known here.
                throw new AceFatalException(
                    $"consumer '{registration.Name}' could not start on queue " +
                    $"'{registration.Queue}'. If the queue does not exist, declare it under " +
                    "acemq:topology:queues, or create it outside the application. " +
                    e.Message,
                    e);
            }

            lock (_running) _running[registration.Name] = handle;

            _log.LogInformation(
                "consumer {Name} consuming {Queue} with {Concurrency} consumer(s), prefetch {Prefetch}",
                registration.Name,
                registration.Queue,
                concurrency,
                registration.Prefetch ?? options.Listener.Prefetch);
        }

        /// <summary>
        /// Stops taking new messages, waits for the handlers already running, and gives up
        /// when whichever deadline arrives first says to.
        /// </summary>
        /// <remarks>
        /// <para>Three things happen, in this order and for reasons that are not
        /// interchangeable.</para>
        ///
        /// <para>First the connection stops handing messages to handlers. A delivery the
        /// broker has already sent is held rather than rejected, so a consumer keeps its
        /// place in the queue. <see cref="AceMqConnection.Held"/> counts those, and they
        /// are the reason a finished drain is not the same thing as an empty queue: they
        /// were fetched and never handled, and the broker redelivers them.</para>
        ///
        /// <para>Then it waits for the handlers that were already running. Each of them
        /// finishes and its decision is carried out — the message is acknowledged, or
        /// retried, or dead-lettered, exactly as it would have been. This is the part worth
        /// having: a handler torn off halfway has already applied whatever side effects it
        /// got to, and the message comes back afterwards to have them applied again.</para>
        ///
        /// <para>Only if that runs out of time does the token handed to the handlers get
        /// cancelled. That is not a way to hurry a drain along; it is what to do once the
        /// drain has already failed, so that the deliveries go back to the broker
        /// unsettled rather than being left with a connection that is about to close. The
        /// messages are redelivered, at the same attempt number, after the restart.</para>
        /// </remarks>
        /// <param name="cancellationToken">
        /// The host's own shutdown deadline, handed straight to the drain. Honoured as well
        /// as the configured one, because the host stops waiting at
        /// <c>HostOptions.ShutdownTimeout</c> whatever this package would have preferred.
        /// Cancellation and overrun are logged as the different events they are: the first
        /// is the host taking its deadline back, the second is the drain having been given
        /// long enough and the handlers not finishing.
        /// </param>
        /// <returns>A task that completes when the drain has finished or given up.</returns>
        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _draining = true;

            List<KeyValuePair<string, IDisposable>> running;
            lock (_running)
            {
                running = _running.ToList();
                _running.Clear();
            }

            if (running.Count == 0)
            {
                _handlers.Cancel();
                return;
            }

            var connection = _connections.Current;
            if (connection == null)
            {
                foreach (var entry in running) Dispose(entry);
                _handlers.Cancel();
                return;
            }

            var budget = _options.Value.Listener.ShutdownTimeout;
            var clock = Stopwatch.StartNew();

            _log.LogInformation(
                "draining {Count} consumer(s); {InFlight} message(s) in flight, {Budget} to finish",
                running.Count,
                connection.InFlight,
                budget);

            // The host's own deadline goes to the drain rather than being raced against it
            // from out here. Cancelling abandons the wait, not the work: the handlers keep
            // running and consuming stays paused, which is why the cancellation has to be
            // followed by the same tidying up as an overrun.
            var drained = false;
            var abandoned = false;
            try
            {
                drained = await connection
                    .DrainConsumersAsync(budget, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Not the same event as the drain running out of time, and logged
                // separately: this one is the host taking its deadline back, and the budget
                // below never got to be the thing that decided.
                abandoned = true;
            }
            catch (Exception e)
            {
                _log.LogWarning(e, "the drain failed");
            }

            if (drained)
            {
                // True means every handler finished. It does not mean every message the
                // broker sent was handled: a delivery fetched and waiting at the pause gate
                // is held, not handled, and comes back from the broker after the restart.
                var held = connection.Held;
                if (held > 0)
                {
                    _log.LogInformation(
                        "drained in {Elapsed}; every handler finished. {Held} delivery(ies) " +
                        "were fetched but never handled — they are unacknowledged and the " +
                        "broker will redeliver them.",
                        clock.Elapsed,
                        held);
                }
                else
                {
                    _log.LogInformation(
                        "drained in {Elapsed}; every handler finished", clock.Elapsed);
                }
            }
            else if (abandoned)
            {
                _log.LogWarning(
                    "the host stopped waiting after {Elapsed}, before the {Budget} the drain " +
                    "was given: {InFlight} handler(s) still running and {Held} delivery(ies) " +
                    "held. Cancelling the handlers; their messages are unacknowledged and the " +
                    "broker will redeliver them. Raise HostOptions.ShutdownTimeout, or lower " +
                    "acemq:listener:shutdownTimeout below it.",
                    clock.Elapsed,
                    budget,
                    connection.InFlight,
                    connection.Held);
            }
            else
            {
                _log.LogWarning(
                    "the drain did not finish within {Budget}: {InFlight} handler(s) still " +
                    "running after {Elapsed}, {Held} delivery(ies) held. Cancelling them; their " +
                    "messages are unacknowledged and the broker will redeliver them.",
                    budget,
                    connection.InFlight,
                    clock.Elapsed,
                    connection.Held);
            }

            // Last, never first.
            _handlers.Cancel();

            foreach (var entry in running) Dispose(entry);
        }

        private void Dispose(KeyValuePair<string, IDisposable> entry)
        {
            try
            {
                entry.Value.Dispose();
            }
            catch (Exception e)
            {
                _log.LogWarning(e, "consumer {Name} did not close cleanly", entry.Key);
            }
        }

        private void WarnIfTheHostWillNotWaitLongEnough(AceMqListenerOptions listener)
        {
            var host = _hostOptions.Value.ShutdownTimeout;
            if (listener.ShutdownTimeout < host) return;

            _log.LogWarning(
                "acemq:listener:shutdownTimeout is {Drain} and the host's ShutdownTimeout is " +
                "{Host}. The host stops waiting first, so the drain never gets the time it was " +
                "given. Set the drain shorter than the host's, or raise the host's.",
                listener.ShutdownTimeout,
                host);
        }
    }
}
