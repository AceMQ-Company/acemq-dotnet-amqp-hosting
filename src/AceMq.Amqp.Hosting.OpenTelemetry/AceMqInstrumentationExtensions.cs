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
using AceMq.Amqp.Hosting;

namespace OpenTelemetry.Trace
{
    /// <summary>Subscribes a tracer to AceMQ's spans.</summary>
    public static class AceMqTracerProviderBuilderExtensions
    {
        /// <summary>
        /// Adds AceMQ's <c>ActivitySource</c> to this tracer.
        /// </summary>
        /// <remarks>
        /// <para>One line rather than nothing at all, and the reason is a dependency this
        /// package does not want to force. <c>AddAceMq</c> cannot subscribe an
        /// OpenTelemetry pipeline it cannot see, and making
        /// <c>AceMq.Amqp.Hosting</c> depend on OpenTelemetry would put it in the
        /// dependency graph of every application that uses AceMQ and not OpenTelemetry.
        /// So the wiring lives here, in the package an application adds precisely because
        /// it has both — which is how every other .NET instrumentation library is
        /// arranged, and means this line looks like the ones around it.</para>
        ///
        /// <para>There is nothing to start and nothing to dispose. The library instruments
        /// itself with the runtime's own <c>ActivitySource</c>, so subscribing to the name
        /// is the whole of it.</para>
        /// </remarks>
        /// <param name="builder">The tracer being built.</param>
        /// <returns>The builder.</returns>
        public static TracerProviderBuilder AddAceMqInstrumentation(
            this TracerProviderBuilder builder)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            return builder.AddSource(AceMqTelemetryNames.ActivitySource);
        }
    }
}

namespace OpenTelemetry.Metrics
{
    /// <summary>Subscribes a meter provider to AceMQ's instruments.</summary>
    public static class AceMqMeterProviderBuilderExtensions
    {
        /// <summary>
        /// Adds AceMQ's <c>Meter</c> to this meter provider.
        /// </summary>
        /// <remarks>
        /// Counters and histograms for publishes, consumes, retries, dead letters, the
        /// outbox and request-reply, all on one meter. The instrument names are on the
        /// library's <c>MetricNames</c>, and they match the Java and Go libraries so one
        /// dashboard serves a polyglot fleet.
        /// </remarks>
        /// <param name="builder">The meter provider being built.</param>
        /// <returns>The builder.</returns>
        public static MeterProviderBuilder AddAceMqInstrumentation(
            this MeterProviderBuilder builder)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            return builder.AddMeter(AceMqTelemetryNames.Meter);
        }
    }
}
