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
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using AceHealthReport = AceMq.Amqp.HealthReport;
using AceHealthStatus = AceMq.Amqp.HealthStatus;

namespace AceMq.Amqp.Hosting
{
    /// <summary>
    /// Reports the connection under the application's health endpoint.
    /// </summary>
    /// <remarks>
    /// <para>Unhealthy means the connection is not open. A broker applying back pressure is
    /// reported as <em>healthy, with the reason</em>, and that is a deliberate choice: a
    /// blocked connection is the broker protecting itself, usually from disk or memory
    /// pressure, and an application that fails its own health check for it is an
    /// application an orchestrator restarts into the same blocked broker, having thrown
    /// away whatever it was holding. The Spring Boot starter reports it the same way, for
    /// the same reason.</para>
    ///
    /// <para><c>HealthStatus.Degraded</c> was the obvious alternative and was
    /// rejected. It maps to 200 in the framework's own defaults, so it would have been
    /// harmless there — but <c>HealthCheckOptions.ResultStatusCodes</c> is routinely
    /// changed to map Degraded to 503, and a choice whose safety depends on a setting in
    /// somebody else's file is not a choice. Blocked goes in the description and the data,
    /// where a dashboard sees it and a restart loop does not.</para>
    ///
    /// <para>Draining <em>is</em> unhealthy, and that is the opposite decision for the
    /// opposite reason: an instance that has been told to stop should leave the rotation
    /// immediately, which is what taking it out of a readiness probe does. The check is
    /// tagged <c>ready</c>, and it should not be used for liveness — a process that cannot
    /// reach its broker is not a process restarting fixes.</para>
    /// </remarks>
    public sealed class AceMqHealthCheck : IHealthCheck
    {
        /// <summary>The name this check is registered under.</summary>
        public const string Name = "acemq";

        private readonly IAceMqConnectionProvider _connections;
        private readonly AceMqConsumerHost _consumers;
        private readonly IOptions<AceMqOptions> _options;

        /// <summary>Constructs the check. Resolved from the container.</summary>
        public AceMqHealthCheck(
            IAceMqConnectionProvider connections,
            AceMqConsumerHost consumers,
            IOptions<AceMqOptions> options)
        {
            _connections = connections ?? throw new ArgumentNullException(nameof(connections));
            _consumers = consumers ?? throw new ArgumentNullException(nameof(consumers));
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        /// <inheritdoc />
        public Task<HealthCheckResult> CheckHealthAsync(
            HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            if (!_options.Value.Enabled)
            {
                return Done(HealthCheckResult.Healthy("acemq:enabled is false"));
            }

            var connection = _connections.Current;
            if (connection == null)
            {
                // Asked before the host has started, or after a connect that failed. Not
                // "the broker is down" — nothing has tried yet — and reporting it unhealthy
                // is right, because the application cannot do its work either way.
                return Done(HealthCheckResult.Unhealthy("not connected"));
            }

            var data = new Dictionary<string, object>
            {
                ["transport"] = connection.TransportName,
                ["open"] = connection.IsOpen,
                ["blocked"] = connection.IsBlocked,
                ["inFlight"] = connection.InFlight,
                ["consumers"] = _consumers.Running.Count,
            };

            if (connection.BlockedReason != null) data["blockedReason"] = connection.BlockedReason;

            if (_consumers.IsDraining)
            {
                return Done(HealthCheckResult.Unhealthy("draining", exception: null, data));
            }

            if (!connection.IsOpen)
            {
                return Done(HealthCheckResult.Unhealthy(
                    "the connection is not open", exception: null, data));
            }

            // Everything the application registered with the connection — an ordered queue
            // with a halted partition, say. The connection's own report is skipped: it
            // calls a blocked connection degraded, and taking the worst of the reports
            // would let that overrule the careful answer above with the plain one.
            var contributors = connection.Health().Reports
                .Where(r => !string.Equals(r.Name, "connection", StringComparison.Ordinal))
                .ToList();

            foreach (var report in contributors)
            {
                data["check." + report.Name] = report.Status.ToString();
            }

            var worst = contributors.Count == 0
                ? AceHealthStatus.Up
                : contributors.Max(r => r.Status);

            if (worst == AceHealthStatus.Down)
            {
                return Done(HealthCheckResult.Unhealthy(
                    Describe(contributors, AceHealthStatus.Down), exception: null, data));
            }

            if (worst == AceHealthStatus.Degraded)
            {
                return Done(HealthCheckResult.Degraded(
                    Describe(contributors, AceHealthStatus.Degraded), exception: null, data));
            }

            if (connection.IsBlocked)
            {
                var reason = connection.BlockedReason ?? "no reason given";
                return Done(HealthCheckResult.Healthy(
                    "the broker has blocked this connection: " + reason, data));
            }

            return Done(HealthCheckResult.Healthy(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "connected over {0}, {1} message(s) in flight",
                    connection.TransportName,
                    connection.InFlight),
                data));
        }

        private static string Describe(
            IEnumerable<AceHealthReport> reports, AceHealthStatus status) =>
            string.Join(", ", reports.Where(r => r.Status == status).Select(r => r.Name)) +
            " reported " + status.ToString().ToLowerInvariant();

        private static Task<HealthCheckResult> Done(HealthCheckResult result) =>
            Task.FromResult(result);
    }
}
