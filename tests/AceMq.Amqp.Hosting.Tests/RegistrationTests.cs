using AceMq.Amqp.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace AceMq.Amqp.Hosting.Tests;

// AceMq.Amqp has a HealthStatus of its own, and this file is in a namespace under
// AceMq.Amqp, so the enclosing namespace would win over a using directive at the
// top of the file. Declared inside the namespace, the alias wins instead. An
// application outside the AceMq.Amqp namespace never meets this.
using HealthStatus = Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus;

/// <summary>
/// What ends up in the container, and in what order.
/// </summary>
public class RegistrationTests
{
    private sealed record Order(string Id);

    private sealed class OrderHandler : IMessageHandler<Order>
    {
        public Task<Ack> HandleAsync(IMessage<Order> message, CancellationToken cancellationToken) =>
            Task.FromResult(Ack.Accept());
    }

    private static IServiceCollection Services()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        return services;
    }

    [Fact]
    public void AddAceMq_registers_the_provider_the_hosts_and_the_check()
    {
        var provider = Services().AddAceMq().Services.BuildServiceProvider();

        Assert.NotNull(provider.GetService<IAceMqConnectionProvider>());
        Assert.NotNull(provider.GetService<AceMqConsumerHost>());
        Assert.NotNull(provider.GetService<AceMqHealthCheck>());

        // AddHealthChecks brings a publisher hosted service of its own; only this
        // package's two are asserted on, and only their order relative to each other.
        var hosted = provider.GetServices<IHostedService>()
            .Where(h => h is AceMqConnectionHost or AceMqConsumerHost)
            .ToList();
        Assert.Collection(
            hosted,
            h => Assert.IsType<AceMqConnectionHost>(h),
            h => Assert.IsType<AceMqConsumerHost>(h));
    }

    [Fact]
    public void The_connection_opens_before_the_consumers_start_and_closes_after_they_drain()
    {
        // Not a property of the classes but of their registration order: the generic host
        // starts hosted services in order and stops them in reverse. If this ever inverts,
        // consumers start against a topology that is not there and are torn down with
        // handlers still running.
        var services = Services().AddAceMq().Services;
        var hosted = services
            .Where(d => d.ServiceType == typeof(IHostedService))
            .ToList();

        Assert.Equal(typeof(AceMqConnectionHost), hosted[0].ImplementationType);
        // The consumer host is registered by factory, so it has no ImplementationType to
        // assert on; that it is the very next one is the property that matters.
        Assert.NotNull(hosted[1].ImplementationFactory);
    }

    [Fact]
    public void The_health_check_is_registered_under_acemq_and_tagged_ready()
    {
        var provider = Services().AddAceMq().Services.BuildServiceProvider();
        var registrations = provider
            .GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;

        var acemq = Assert.Single(registrations, r => r.Name == "acemq");
        Assert.Contains("ready", acemq.Tags);
        Assert.Contains("acemq", acemq.Tags);
    }

    [Fact]
    public void Calling_AddAceMq_twice_does_not_double_the_hosted_services()
    {
        var services = Services();
        services.AddAceMq();
        services.AddAceMq(configure: o => o.Url = "amqp://second:5672");

        var provider = services.BuildServiceProvider();

        Assert.Equal(
            2,
            provider.GetServices<IHostedService>()
                .Count(h => h is AceMqConnectionHost or AceMqConsumerHost));
        Assert.Single(provider.GetServices<AceMqHealthCheck>());
        Assert.Equal(
            "amqp://second:5672",
            provider.GetRequiredService<IOptions<AceMqOptions>>().Value.Url);
    }

    [Fact]
    public void A_handler_type_is_registered_scoped_by_default()
    {
        var services = Services();
        services.AddAceMq().AddConsumer<Order, OrderHandler>("orders.new");

        var descriptor = Assert.Single(services, d => d.ServiceType == typeof(OrderHandler));
        Assert.Equal(ServiceLifetime.Scoped, descriptor.Lifetime);
    }

    [Fact]
    public void A_handler_can_ask_to_be_a_singleton()
    {
        var services = Services();
        services.AddAceMq()
            .AddConsumer<Order, OrderHandler>("orders.new", lifetime: ServiceLifetime.Singleton);

        var descriptor = Assert.Single(services, d => d.ServiceType == typeof(OrderHandler));
        Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
    }

    [Fact]
    public void A_registration_records_the_queue_the_type_and_the_name()
    {
        var provider = Services()
            .AddAceMq()
            .AddConsumer<Order, OrderHandler>("orders.new")
            .Services.BuildServiceProvider();

        var registration = Assert.Single(provider.GetServices<AceMqConsumerRegistration>());

        Assert.Equal("OrderHandler", registration.Name);
        Assert.Equal("orders.new", registration.Queue);
        Assert.Equal(typeof(Order), registration.MessageType);
        Assert.Null(registration.Prefetch);
        Assert.Equal(ServiceLifetime.Scoped, registration.HandlerLifetime);
    }

    [Fact]
    public void Per_consumer_overrides_are_recorded_and_defaulted_from_the_listener()
    {
        var provider = Services()
            .AddAceMq(configure: o =>
            {
                o.Listener.Prefetch = 100;
                o.Listener.Concurrency = 1;
            })
            .AddConsumer<Order, OrderHandler>(
                "orders.new",
                configure: r =>
                {
                    r.Prefetch = 5;
                    r.Concurrency = 3;
                    r.Retry = new AceMqRetryOptions { Enabled = true, MaxAttempts = 7 };
                })
            .Services.BuildServiceProvider();

        var registration = Assert.Single(provider.GetServices<AceMqConsumerRegistration>());
        var options = provider.GetRequiredService<IOptions<AceMqOptions>>().Value;

        var consumerOptions = AceMqConnections.ConsumerOptionsFrom(
            options.Listener, registration, codec: null);

        Assert.Equal(5, consumerOptions.PrefetchCount);
        Assert.Equal(7, consumerOptions.RetryPolicy!.MaxAttempts);
        Assert.Equal(3, registration.Concurrency);
    }

    [Fact]
    public void Several_consumers_accumulate()
    {
        var provider = Services()
            .AddAceMq()
            .AddConsumer<Order, OrderHandler>("orders.new")
            .AddConsumer<Order>("orders.cancelled", (_, _) => Task.FromResult(Ack.Accept()))
            .Services.BuildServiceProvider();

        var registrations = provider.GetServices<AceMqConsumerRegistration>().ToList();

        Assert.Equal(2, registrations.Count);
        Assert.Equal("OrderHandler", registrations[0].Name);
        Assert.Equal("orders.cancelled", registrations[1].Name);
    }

    [Fact]
    public async Task Two_consumers_with_the_same_name_are_refused_at_start()
    {
        var provider = Services()
            .AddAceMq()
            .AddConsumer<Order, OrderHandler>("orders.new", name: "orders")
            .AddConsumer<Order>(
                "orders.cancelled", (_, _) => Task.FromResult(Ack.Accept()), name: "orders")
            .Services.BuildServiceProvider();

        var host = provider.GetRequiredService<AceMqConsumerHost>();

        var failure = await Assert.ThrowsAsync<InvalidOperationException>(
            () => host.StartAsync(CancellationToken.None));
        Assert.Contains("share the name 'orders'", failure.Message);
    }

    [Fact]
    public void A_consumer_needs_a_queue()
    {
        Assert.Throws<ArgumentException>(
            () => Services().AddAceMq().AddConsumer<Order, OrderHandler>("  "));
    }

    [Fact]
    public async Task Disabled_means_nothing_connects_and_nothing_starts()
    {
        var provider = Services()
            .AddAceMq(configure: o =>
            {
                o.Enabled = false;
                o.Url = "amqp://nowhere.invalid:5672";
            })
            .AddConsumer<Order, OrderHandler>("orders.new")
            .Services.BuildServiceProvider();

        // Would throw on a connect attempt; the point is that there is not one.
        await provider.GetRequiredService<AceMqConsumerHost>().StartAsync(CancellationToken.None);
        Assert.Empty(provider.GetRequiredService<AceMqConsumerHost>().Running);

        var health = await provider.GetRequiredService<AceMqHealthCheck>()
            .CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Healthy, health.Status);
    }

    [Fact]
    public void The_client_name_defaults_to_the_application()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddSingleton<IHostEnvironment>(
            new HostingEnvironment { ApplicationName = "orders-worker" });
        services.AddAceMq();

        var options = services.BuildServiceProvider()
            .GetRequiredService<IOptions<AceMqOptions>>().Value;

        Assert.Equal("orders-worker", options.ClientName);
    }

    [Fact]
    public void A_configured_client_name_wins_over_the_application_name()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddSingleton<IHostEnvironment>(
            new HostingEnvironment { ApplicationName = "orders-worker" });
        services.AddAceMq(configure: o => o.ClientName = "explicit");

        var options = services.BuildServiceProvider()
            .GetRequiredService<IOptions<AceMqOptions>>().Value;

        Assert.Equal("explicit", options.ClientName);
    }

    private sealed class HostingEnvironment : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = "Test";
        public string ApplicationName { get; set; } = "test";
        public string ContentRootPath { get; set; } = ".";
        public Microsoft.Extensions.FileProviders.IFileProvider ContentRootFileProvider { get; set; } =
            new Microsoft.Extensions.FileProviders.NullFileProvider();
    }
}
