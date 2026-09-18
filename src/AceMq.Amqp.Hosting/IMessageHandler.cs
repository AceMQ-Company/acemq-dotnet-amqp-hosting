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

using System.Threading;
using System.Threading.Tasks;

namespace AceMq.Amqp.Hosting
{
    /// <summary>
    /// Handles messages of one type from one queue.
    /// </summary>
    /// <typeparam name="T">The payload type the codec decodes into.</typeparam>
    /// <remarks>
    /// <para>An interface resolved from the container rather than an attribute discovered
    /// by scanning, which is the choice the Spring starter made in the other direction.
    /// The reasoning is in the README; the short version is that a handler here is an
    /// ordinary service with ordinary constructor injection, resolved from a scope per
    /// message, and everything the container already does — options, decorators,
    /// <c>IServiceScope</c>-lifetime dependencies like a DbContext — applies to it without
    /// this package knowing anything about them.</para>
    ///
    /// <para>The <c>cancellationToken</c> is <em>not</em> the host's stopping
    /// token. It stays uncancelled through a graceful drain, precisely so a handler that
    /// respects it finishes its message rather than abandoning it. It is cancelled only
    /// when the drain has already run out of time, which is the signal to stop and let the
    /// broker redeliver. See the lifecycle page.</para>
    /// </remarks>
    public interface IMessageHandler<T>
    {
        /// <summary>
        /// Handles one message and says what should happen to it.
        /// </summary>
        /// <param name="message">The decoded message and its envelope.</param>
        /// <param name="cancellationToken">
        /// Cancelled when a drain has overrun and the message should be given back to the
        /// broker rather than finished.
        /// </param>
        /// <returns>
        /// What to do with the delivery. Returning nothing sensible is not an option;
        /// <see cref="Ack.Accept"/> is the common answer and <see cref="Ack.Retry(string)"/>,
        /// <see cref="Ack.DeadLetter"/>, <see cref="Ack.Release"/> and
        /// <see cref="Ack.Park"/> are the others. Throwing is also allowed and is treated
        /// as a failure by the library's own rules.
        /// </returns>
        Task<Ack> HandleAsync(IMessage<T> message, CancellationToken cancellationToken);
    }
}
