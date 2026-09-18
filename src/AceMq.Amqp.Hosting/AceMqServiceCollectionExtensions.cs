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
using System.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace AceMq.Amqp.Hosting
{
    /// <summary>
    /// What an application calls to configure AceMQ.
    /// </summary>
    public static class AceMqServiceCollectionExtensions
    {
        /// <summary>
        /// Registers AceMQ, binding the <c>acemq</c> section of the application's
        /// configuration.
        /// </summary>
        /// <remarks>
        /// Idempotent in the way DI registrations are expected to be: calling it twice
        /// does not register the hosted services twice, and the second call's
        /// <paramref name="configure"/> runs after the first's. Consumers, by contrast,
        /// accumulate — two <c>AddConsumer</c> calls mean two consumers, which is the
        /// whole point of them.
        /// </remarks>
        /// <param name="services">The application's services.</param>
        /// <param name="configure">Changes applied after the configuration is bound.</param>
        /// <returns>A builder, for registering consumers.</returns>
        public static IAceMqBuilder AddAceMq(
            this IServiceCollection services, Action<AceMqOptions>? configure = null) =>
            services.AddAceMq(configuration: null, configure);

        /// <summary>
        /// Registers AceMQ, binding a configuration section chosen by the caller.
        /// </summary>
        /// <param name="services">The application's services.</param>
        /// <param name="configuration">
        /// The section to bind — <c>configuration.GetSection("messaging")</c>, say. Null
        /// binds the <c>acemq</c> section of whatever <see cref="IConfiguration"/> the
        /// container has.
        /// </param>
        /// <param name="configure">Changes applied after the configuration is bound.</param>
        /// <returns>A builder, for registering consumers.</returns>
        public static IAceMqBuilder AddAceMq(
            this IServiceCollection services,
            IConfiguration? configuration,
            Action<AceMqOptions>? configure = null)
        {
            if (services == null) throw new ArgumentNullException(nameof(services));

            var already = services.Any(d => d.ServiceType == typeof(IAceMqConnectionProvider));

            if (!already)
            {
                services.AddOptions();
                services.AddLogging();

                if (configuration != null)
                {
                    services.Configure<AceMqOptions>(configuration);
                }
                else
                {
                    // Bound from the container's IConfiguration rather than captured here,
                    // so an application that adds a configuration source after AddAceMq —
                    // which is the normal order in a minimal host — is still bound from it.
                    services.AddSingleton<IConfigureOptions<AceMqOptions>>(sp =>
                        new ConfigureFromConfigurationOptions(
                            sp.GetService<IConfiguration>()?.GetSection(AceMqOptions.SectionName)));
                }

                // The application's name, unless the configuration said otherwise. An
                // unnamed connection on a broker page of forty is the same as no name.
                services.AddSingleton<IPostConfigureOptions<AceMqOptions>>(sp =>
                    new PostConfigureClientName(sp.GetService<IHostEnvironment>()));

                services.AddSingleton<IValidateOptions<AceMqOptions>, AceMqOptionsValidator>();

                services.TryAddSingleton<AceMqConnectionProvider>();
                services.TryAddSingleton<IAceMqConnectionProvider>(
                    sp => sp.GetRequiredService<AceMqConnectionProvider>());

                // For the code that would rather inject the connection than await a
                // provider. It blocks, which is safe here and everywhere a modern host
                // runs — there is no synchronization context to deadlock against — but it
                // does block, so a constructor that resolves it before the host has
                // started pays for the connection there. AceMqConnectionHost opens it
                // first in the ordinary case, so in practice this hands back a connection
                // that is already open.
                services.TryAddSingleton(sp =>
                    sp.GetRequiredService<IAceMqConnectionProvider>()
                        .GetAsync()
                        .ConfigureAwait(false)
                        .GetAwaiter()
                        .GetResult());

                services.TryAddSingleton<AceMqConsumerHost>();

                // Order matters and is the only thing that makes this correct: hosted
                // services start in registration order and stop in reverse, so the
                // connection opens before the consumers start and closes after they have
                // drained.
                services.AddSingleton<IHostedService, AceMqConnectionHost>();
                services.AddSingleton<IHostedService>(
                    sp => sp.GetRequiredService<AceMqConsumerHost>());

                services.AddAceMqHealthCheck();
            }

            if (configuration != null && already)
            {
                services.Configure<AceMqOptions>(configuration);
            }

            if (configure != null) services.Configure(configure);

            return new AceMqBuilder(services);
        }

        private static void AddAceMqHealthCheck(this IServiceCollection services)
        {
            // Registered here rather than left to the application, because a health check
            // nobody remembered to add is the one that would have been useful. It is
            // tagged "ready" so an application that separates its probes gets the right
            // answer without configuring anything — and tagged "acemq" so one that wants
            // to exclude it can.
            services.AddSingleton<AceMqHealthCheck>();
            services.AddHealthChecks().AddCheck<AceMqHealthCheck>(
                AceMqHealthCheck.Name,
                failureStatus: null,
                tags: new[] { "acemq", "ready" });
        }

        private sealed class ConfigureFromConfigurationOptions : IConfigureOptions<AceMqOptions>
        {
            private readonly IConfiguration? _section;

            public ConfigureFromConfigurationOptions(IConfiguration? section) => _section = section;

            public void Configure(AceMqOptions options) => _section?.Bind(options);
        }

        private sealed class PostConfigureClientName : IPostConfigureOptions<AceMqOptions>
        {
            private readonly IHostEnvironment? _environment;

            public PostConfigureClientName(IHostEnvironment? environment) =>
                _environment = environment;

            public void PostConfigure(string? name, AceMqOptions options)
            {
                if (!string.IsNullOrEmpty(options.ClientName)) return;
                if (_environment == null) return;
                options.ClientName = _environment.ApplicationName;
            }
        }
    }

    /// <summary>
    /// The handle <c>AddAceMq</c> returns, for registering consumers against.
    /// </summary>
    public interface IAceMqBuilder
    {
        /// <summary>The services being configured.</summary>
        IServiceCollection Services { get; }
    }

    internal sealed class AceMqBuilder : IAceMqBuilder
    {
        public AceMqBuilder(IServiceCollection services) => Services = services;

        public IServiceCollection Services { get; }
    }
}
