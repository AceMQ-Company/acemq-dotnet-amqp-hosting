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

namespace AceMq.Amqp.Hosting
{
    /// <summary>
    /// The names of the <c>ActivitySource</c> and <c>Meter</c> the library publishes.
    /// </summary>
    /// <remarks>
    /// <para>The library instruments itself with the runtime's own
    /// <see cref="System.Diagnostics.ActivitySource"/> and
    /// <see cref="System.Diagnostics.Metrics.Meter"/>, so there is nothing to start and
    /// nothing to inject — a collector that subscribes to these two names sees everything.
    /// Both are already <c>public const</c> on the library's <c>MetricNames</c>; they are
    /// repeated here so that a <c>.AddSource(...)</c> line can be written without a
    /// <c>using AceMq.Amqp;</c> that the file otherwise does not need.</para>
    ///
    /// <para><c>AceMq.Amqp.Hosting.OpenTelemetry</c> turns these into
    /// <c>AddAceMqInstrumentation()</c> on the OpenTelemetry builders, which is what most
    /// applications should use. These constants are for everyone else — a hand-rolled
    /// <c>MeterListener</c>, an <c>ActivityListener</c> in a test, a diagnostic tool.</para>
    /// </remarks>
    public static class AceMqTelemetryNames
    {
        /// <summary>The <c>ActivitySource</c> spans are emitted on.</summary>
        public const string ActivitySource = MetricNames.ActivitySource;

        /// <summary>The <c>Meter</c> instruments are published on.</summary>
        public const string Meter = MetricNames.Meter;
    }
}
