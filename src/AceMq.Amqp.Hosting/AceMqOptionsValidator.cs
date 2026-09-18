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
using Microsoft.Extensions.Options;

namespace AceMq.Amqp.Hosting
{
    /// <summary>
    /// Refuses configuration that would fail later and less clearly.
    /// </summary>
    /// <remarks>
    /// Every rule here exists because the failure it prevents is otherwise reported by
    /// something several layers down, in terms that do not name the setting. A prefetch of
    /// zero, for instance, is a consumer that receives nothing and says nothing about why.
    /// </remarks>
    internal sealed class AceMqOptionsValidator : IValidateOptions<AceMqOptions>
    {
        public ValidateOptionsResult Validate(string? name, AceMqOptions options)
        {
            if (!options.Enabled) return ValidateOptionsResult.Success;

            var failures = new List<string>();

            if (string.IsNullOrWhiteSpace(options.Url))
            {
                failures.Add("acemq:url is required");
            }
            else if (options.Url.IndexOf("://", StringComparison.Ordinal) <= 0 ||
                     !Uri.TryCreate(options.Url, UriKind.Absolute, out _))
            {
                // The "://" is checked separately because Uri.TryCreate accepts
                // "broker:5672" as absolute, reading "broker" as the scheme. The scheme is
                // what picks the transport, so a URL without one fails much later with
                // "no transport registered for scheme 'broker'" — which is true, and is
                // not the problem.
                failures.Add(
                    $"acemq:url is not a broker URL: '{options.Url}'. " +
                    "It needs a scheme — amqp://host:5672 or amqps://host:5671.");
            }

            if (options.MaxOutstandingPublishes < 1)
            {
                failures.Add("acemq:maxOutstandingPublishes must be at least 1");
            }

            if (options.ConnectionTimeout <= TimeSpan.Zero)
            {
                failures.Add("acemq:connectionTimeout must be positive");
            }

            if (options.ConfirmTimeout <= TimeSpan.Zero)
            {
                failures.Add("acemq:confirmTimeout must be positive");
            }

            if (options.Listener.Prefetch < 1)
            {
                failures.Add("acemq:listener:prefetch must be at least 1");
            }

            if (options.Listener.Concurrency < 1)
            {
                failures.Add("acemq:listener:concurrency must be at least 1");
            }

            if (options.Listener.ShutdownTimeout < TimeSpan.Zero)
            {
                failures.Add("acemq:listener:shutdownTimeout cannot be negative");
            }

            var retry = options.Listener.Retry;
            if (retry.Enabled)
            {
                if (retry.MaxAttempts < 1)
                {
                    failures.Add("acemq:listener:retry:maxAttempts must be at least 1");
                }

                if (retry.InitialDelay <= TimeSpan.Zero)
                {
                    failures.Add("acemq:listener:retry:initialDelay must be positive");
                }

                if (retry.MaxDelay < retry.InitialDelay)
                {
                    failures.Add(
                        "acemq:listener:retry:maxDelay is shorter than the initial delay");
                }

                if (retry.Multiplier < 1)
                {
                    failures.Add(
                        "acemq:listener:retry:multiplier below 1 makes each retry sooner " +
                        "than the last, which is the opposite of a backoff");
                }

                if (retry.Jitter < 0 || retry.Jitter > 1)
                {
                    failures.Add("acemq:listener:retry:jitter is a fraction between 0 and 1");
                }
            }

            foreach (var queue in options.Topology.Queues)
            {
                if (string.IsNullOrWhiteSpace(queue.Name))
                {
                    failures.Add("every entry in acemq:topology:queues needs a name");
                }
            }

            foreach (var exchange in options.Topology.Exchanges)
            {
                if (string.IsNullOrWhiteSpace(exchange.Name))
                {
                    failures.Add("every entry in acemq:topology:exchanges needs a name");
                }
            }

            foreach (var binding in options.Topology.Bindings)
            {
                if (string.IsNullOrWhiteSpace(binding.Queue) ||
                    string.IsNullOrWhiteSpace(binding.Exchange))
                {
                    failures.Add(
                        "every entry in acemq:topology:bindings needs a queue and an exchange");
                }
            }

            return failures.Count == 0
                ? ValidateOptionsResult.Success
                : ValidateOptionsResult.Fail(failures);
        }
    }
}
