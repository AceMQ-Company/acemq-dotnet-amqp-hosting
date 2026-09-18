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
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AceMq.Amqp.Hosting
{
    /// <summary>
    /// Opens the connection and declares the topology, before any consumer starts.
    /// </summary>
    /// <remarks>
    /// <para>Registered before <see cref="AceMqConsumerHost"/>. Hosted services start in
    /// registration order and stop in reverse, so this opens first and closes last, and a
    /// consumer never starts against a topology that is not there yet.</para>
    ///
    /// <para>It has nothing to do at shutdown. The connection is owned by
    /// <see cref="IAceMqConnectionProvider"/> and disposed by the container, which happens
    /// after every hosted service has stopped — that is, after the drain.</para>
    /// </remarks>
    public sealed class AceMqConnectionHost : IHostedService
    {
        private readonly IAceMqConnectionProvider _connections;
        private readonly IOptions<AceMqOptions> _options;
        private readonly ILogger<AceMqConnectionHost> _log;

        /// <summary>Constructs the host. Resolved from the container.</summary>
        public AceMqConnectionHost(
            IAceMqConnectionProvider connections,
            IOptions<AceMqOptions> options,
            ILogger<AceMqConnectionHost> log)
        {
            _connections = connections ?? throw new ArgumentNullException(nameof(connections));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _log = log ?? throw new ArgumentNullException(nameof(log));
        }

        /// <inheritdoc />
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var options = _options.Value;
            if (!options.Enabled)
            {
                _log.LogInformation("acemq:enabled is false; no connection and no consumers");
                return;
            }

            var connection = await _connections.GetAsync(cancellationToken).ConfigureAwait(false);

            var topology = AceMqConnections.TopologyFrom(options.Topology);
            if (topology == null)
            {
                _log.LogDebug("no topology declared; nothing to apply");
                return;
            }

            var mode = options.Topology.Apply == TopologyApply.DryRun
                ? ApplyMode.DryRun
                : ApplyMode.Declare;

            var plan = await connection.ApplyAsync(topology, mode).ConfigureAwait(false);

            if (mode == ApplyMode.DryRun)
            {
                _log.LogInformation("topology dry run:{NewLine}{Plan}", Environment.NewLine, plan.Render());
            }
            else if (plan.HasChanges)
            {
                _log.LogInformation("topology applied:{NewLine}{Plan}", Environment.NewLine, plan.Render());
            }

            if (!plan.HasDrift) return;

            // Drift is a queue or exchange that exists with settings a declaration cannot
            // change. Declaring over it is not an option the broker offers, so the only
            // question is whether to say so and carry on or to refuse to start.
            if (options.Topology.FailOnDrift)
            {
                throw new AceFatalException(
                    "the broker's topology has drifted from the declared one and " +
                    "acemq:topology:failOnDrift is set:" + Environment.NewLine + plan.Render());
            }

            _log.LogWarning(
                "the broker's topology has drifted from the declared one:{NewLine}{Plan}",
                Environment.NewLine,
                plan.Render());
        }

        /// <inheritdoc />
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
