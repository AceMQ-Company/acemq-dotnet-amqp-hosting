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
    /// Registers consumers on the builder <c>AddAceMq</c> returned.
    /// </summary>
    public static class AceMqBuilderExtensions
    {
        /// <summary>
        /// Registers a handler type to consume a queue.
        /// </summary>
        /// <remarks>
        /// <para>The handler is resolved from a fresh <see cref="IServiceScope"/> for every
        /// message, so scoped dependencies — a DbContext, a unit of work, a request-scoped
        /// tenant — behave the way they do everywhere else in the application, and two
        /// messages handled at once never share one.</para>
        ///
        /// <para>A handler that must keep state between messages can be registered as a
        /// singleton with <paramref name="lifetime"/>; it is then resolved once and shared,
        /// and making it thread-safe becomes its own problem.</para>
        /// </remarks>
        /// <typeparam name="TMessage">The payload type the codec decodes into.</typeparam>
        /// <typeparam name="THandler">The handler.</typeparam>
        /// <param name="builder">The builder.</param>
        /// <param name="queue">The queue to consume.</param>
        /// <param name="name">
        /// What this consumer is called in logs. Defaults to the handler's type name, and
        /// must be unique across the application.
        /// </param>
        /// <param name="lifetime">The lifetime the handler is registered with.</param>
        /// <param name="configure">Settings that differ from the listener defaults.</param>
        /// <returns>The builder.</returns>
        public static IAceMqBuilder AddConsumer<TMessage, THandler>(
            this IAceMqBuilder builder,
            string queue,
            string? name = null,
            ServiceLifetime lifetime = ServiceLifetime.Scoped,
            Action<AceMqConsumerRegistration>? configure = null)
            where THandler : class, IMessageHandler<TMessage>
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            RequireQueue(queue);

            builder.Services.Add(new ServiceDescriptor(typeof(THandler), typeof(THandler), lifetime));

            return builder.Add<TMessage>(
                name ?? typeof(THandler).Name,
                queue,
                lifetime,
                configure,
                (services, handlerToken) => async message =>
                {
                    using (var scope = services.CreateScope())
                    {
                        var handler = scope.ServiceProvider.GetRequiredService<THandler>();
                        return await handler
                            .HandleAsync(message, handlerToken)
                            .ConfigureAwait(false);
                    }
                });
        }

        /// <summary>
        /// Registers a delegate to consume a queue.
        /// </summary>
        /// <remarks>
        /// For the handler that is three lines and has no dependencies. It is invoked
        /// directly, with no scope created around it — there is nothing to resolve — so a
        /// delegate that wants a scoped service should take <see cref="IServiceProvider"/>
        /// from its closure and make its own, or be a handler type instead.
        /// </remarks>
        /// <typeparam name="TMessage">The payload type the codec decodes into.</typeparam>
        /// <param name="builder">The builder.</param>
        /// <param name="queue">The queue to consume.</param>
        /// <param name="handler">The handler.</param>
        /// <param name="name">What this consumer is called in logs. Defaults to the queue.</param>
        /// <param name="configure">Settings that differ from the listener defaults.</param>
        /// <returns>The builder.</returns>
        public static IAceMqBuilder AddConsumer<TMessage>(
            this IAceMqBuilder builder,
            string queue,
            Func<IMessage<TMessage>, CancellationToken, Task<Ack>> handler,
            string? name = null,
            Action<AceMqConsumerRegistration>? configure = null)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            if (handler == null) throw new ArgumentNullException(nameof(handler));
            RequireQueue(queue);

            return builder.Add<TMessage>(
                name ?? queue,
                queue,
                ServiceLifetime.Singleton,
                configure,
                (services, handlerToken) => message => handler(message, handlerToken));
        }

        private static IAceMqBuilder Add<TMessage>(
            this IAceMqBuilder builder,
            string name,
            string queue,
            ServiceLifetime lifetime,
            Action<AceMqConsumerRegistration>? configure,
            Func<IServiceProvider, CancellationToken, Func<IMessage<TMessage>, Task<Ack>>> handler)
        {
            var registration = new AceMqConsumerRegistration(
                name,
                queue,
                typeof(TMessage),
                async (connection, services, options, concurrency, handlerToken) =>
                    // A ConsumerGroup even at concurrency one, as the Spring starter does.
                    // One type means one shutdown path and one set of counters to read, and
                    // the group of one costs nothing.
                    (IDisposable)await ConsumerGroup
                        .StartAsync(
                            connection,
                            queue,
                            concurrency,
                            options,
                            handler(services, handlerToken))
                        .ConfigureAwait(false))
            {
                HandlerLifetime = lifetime,
            };

            configure?.Invoke(registration);
            builder.Services.AddSingleton(registration);
            return builder;
        }

        private static void RequireQueue(string queue)
        {
            if (string.IsNullOrWhiteSpace(queue))
            {
                throw new ArgumentException("a consumer needs a queue", nameof(queue));
            }
        }
    }
}
