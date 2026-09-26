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
using Microsoft.Extensions.DependencyInjection;

namespace AceMq.Amqp.Hosting
{
    /// <summary>
    /// One registered consumer: the queue, the settings that differ from the defaults, and
    /// the closure that starts it.
    /// </summary>
    /// <remarks>
    /// Registered as a singleton in the container, one instance per
    /// <c>AddConsumer</c> call, and read back as an <c>IEnumerable</c> by
    /// <see cref="AceMqConsumerHost"/>. It is public because an application may want to
    /// read what it registered — to assert on it in a test, or to log it — and because
    /// the host has to be able to be replaced.
    /// </remarks>
    public sealed class AceMqConsumerRegistration
    {
        internal AceMqConsumerRegistration(
            string name,
            string queue,
            Type messageType,
            Func<AceMqConnection, IServiceProvider, ConsumerOptions, int, CancellationToken, Task<IDisposable>> start)
        {
            Name = name;
            Queue = queue;
            MessageType = messageType;
            Start = start;
        }

        /// <summary>
        /// The name this consumer is known by in logs and in
        /// <see cref="AceMqConsumerHost.Running"/>. Defaults to the handler type's name,
        /// or to the queue for a delegate handler.
        /// </summary>
        public string Name { get; }

        /// <summary>The queue it consumes.</summary>
        public string Queue { get; }

        /// <summary>The payload type the codec decodes into.</summary>
        public Type MessageType { get; }

        /// <summary>Unacknowledged messages allowed per consumer. Unset, the listener default.</summary>
        public int? Prefetch { get; set; }

        /// <summary>Consumers to run. Unset, the listener default.</summary>
        public int? Concurrency { get; set; }

        /// <summary>
        /// Start with the host. Unset, the listener default. Off, the consumer is
        /// registered and idle until <see cref="AceMqConsumerHost.StartAsync(string)"/>
        /// is called for it.
        /// </summary>
        public bool? AutoStartup { get; set; }

        /// <summary>Requeue on failure instead of dead-lettering. Unset, the listener default.</summary>
        public bool? RequeueOnFailure { get; set; }

        /// <summary>The retry ladder for this consumer. Unset, the listener default.</summary>
        public AceMqRetryOptions? Retry { get; set; }

        /// <summary>
        /// The store that decides whether this message has been handled before. Unset,
        /// nothing deduplicates and a redelivery is handled again.
        /// </summary>
        /// <remarks>
        /// <para>A factory rather than a store, because <c>AddConsumer</c> runs while the
        /// container is still being built and a store worth having is a service in it — the
        /// same database the handler writes to, so that the claim and the work commit or
        /// roll back together. The factory is called once per consumer, when the consumer
        /// starts, with the application's root <see cref="IServiceProvider"/>.</para>
        ///
        /// <para>There is no configuration key for this and there will not be one. A store
        /// is a connection string, a table name and a retention window at minimum, and
        /// naming one in <c>appsettings.json</c> would mean this package building database
        /// connections — which is the application's business and not a message library's.
        /// </para>
        ///
        /// <para>A store that is already an object is written <c>_ => store</c>.</para>
        /// </remarks>
        public Func<IServiceProvider, IIdempotencyStore>? Idempotency { get; set; }

        /// <summary>The lifetime the handler is resolved with. Scoped unless changed.</summary>
        public ServiceLifetime HandlerLifetime { get; internal set; } = ServiceLifetime.Scoped;

        internal Func<AceMqConnection, IServiceProvider, ConsumerOptions, int, CancellationToken, Task<IDisposable>> Start { get; }
    }
}
