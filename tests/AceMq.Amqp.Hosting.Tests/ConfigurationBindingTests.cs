using AceMq.Amqp.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace AceMq.Amqp.Hosting.Tests;

/// <summary>
/// The configuration section, and what it turns into.
/// </summary>
/// <remarks>
/// The mapping from settings to <see cref="ConnectionConfig"/> is where an integration
/// package earns its keep and where a typo is invisible until production, so it is tested
/// through the real binder rather than by setting properties by hand.
/// </remarks>
public class ConfigurationBindingTests
{
    private static AceMqOptions Bind(params (string Key, string Value)[] settings)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(settings.Select(s =>
                new KeyValuePair<string, string?>(s.Key, s.Value)))
            .Build();

        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddAceMq();
        return services.BuildServiceProvider()
            .GetRequiredService<IOptions<AceMqOptions>>().Value;
    }

    [Fact]
    public void Defaults_are_a_local_broker_with_confirms_on()
    {
        var options = Bind();

        Assert.True(options.Enabled);
        Assert.Equal("amqp://localhost:5672", options.Url);
        Assert.True(options.PublisherConfirms);
        Assert.Equal(10_000, options.MaxOutstandingPublishes);
        Assert.Equal("json", options.Format);
        Assert.Equal(100, options.Listener.Prefetch);
        Assert.Equal(1, options.Listener.Concurrency);
        Assert.False(options.Listener.Retry.Enabled);
        Assert.Equal(AceMqTlsMode.Disabled, options.Tls.Mode);
        Assert.True(options.Topology.IsEmpty);
    }

    [Fact]
    public void Every_scalar_binds_from_the_acemq_section()
    {
        var options = Bind(
            ("acemq:url", "amqp://broker:5672"),
            ("acemq:username", "svc"),
            ("acemq:password", "hunter2"),
            ("acemq:virtualHost", "/orders"),
            ("acemq:clientName", "orders-worker"),
            ("acemq:connectionTimeout", "00:00:05"),
            ("acemq:confirmTimeout", "00:00:45"),
            ("acemq:publisherConfirms", "false"),
            ("acemq:maxOutstandingPublishes", "512"),
            ("acemq:format", "xml"));

        Assert.Equal("amqp://broker:5672", options.Url);
        Assert.Equal("svc", options.Username);
        Assert.Equal("hunter2", options.Password);
        Assert.Equal("/orders", options.VirtualHost);
        Assert.Equal("orders-worker", options.ClientName);
        Assert.Equal(TimeSpan.FromSeconds(5), options.ConnectionTimeout);
        Assert.Equal(TimeSpan.FromSeconds(45), options.ConfirmTimeout);
        Assert.False(options.PublisherConfirms);
        Assert.Equal(512, options.MaxOutstandingPublishes);
        Assert.Equal("xml", options.Format);
    }

    [Fact]
    public void Listener_defaults_and_the_retry_ladder_bind()
    {
        var options = Bind(
            ("acemq:listener:prefetch", "20"),
            ("acemq:listener:concurrency", "4"),
            ("acemq:listener:autoStartup", "false"),
            ("acemq:listener:shutdownTimeout", "00:00:12"),
            ("acemq:listener:requeueOnFailure", "true"),
            ("acemq:listener:retry:enabled", "true"),
            ("acemq:listener:retry:maxAttempts", "5"),
            ("acemq:listener:retry:initialDelay", "00:00:02"),
            ("acemq:listener:retry:maxDelay", "00:05:00"),
            ("acemq:listener:retry:multiplier", "3"),
            ("acemq:listener:retry:jitter", "0.25"),
            ("acemq:listener:retry:giveUpAfter", "01:00:00"),
            ("acemq:listener:retry:brokerWaitThreshold", "00:00:10"));

        var listener = options.Listener;
        Assert.Equal(20, listener.Prefetch);
        Assert.Equal(4, listener.Concurrency);
        Assert.False(listener.AutoStartup);
        Assert.Equal(TimeSpan.FromSeconds(12), listener.ShutdownTimeout);
        Assert.True(listener.RequeueOnFailure);

        var retry = listener.Retry;
        Assert.True(retry.Enabled);
        Assert.Equal(5, retry.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(2), retry.InitialDelay);
        Assert.Equal(TimeSpan.FromMinutes(5), retry.MaxDelay);
        Assert.Equal(3.0, retry.Multiplier);
        Assert.Equal(0.25, retry.Jitter);
        Assert.Equal(TimeSpan.FromHours(1), retry.GiveUpAfter);
        Assert.Equal(TimeSpan.FromSeconds(10), retry.BrokerWaitThreshold);
    }

    [Fact]
    public void Topology_binds_as_lists()
    {
        var options = Bind(
            ("acemq:topology:exchanges:0:name", "orders"),
            ("acemq:topology:exchanges:0:type", "topic"),
            ("acemq:topology:queues:0:name", "orders.new"),
            ("acemq:topology:queues:0:type", "Quorum"),
            ("acemq:topology:queues:0:deadLetter", "true"),
            ("acemq:topology:queues:0:arguments:x-max-length", "1000"),
            ("acemq:topology:bindings:0:queue", "orders.new"),
            ("acemq:topology:bindings:0:exchange", "orders"),
            ("acemq:topology:bindings:0:routingKey", "order.created"));

        Assert.False(options.Topology.IsEmpty);
        Assert.Equal("orders", options.Topology.Exchanges[0].Name);
        Assert.Equal("topic", options.Topology.Exchanges[0].Type);
        Assert.Equal("orders.new", options.Topology.Queues[0].Name);
        Assert.Equal(QueueType.Quorum, options.Topology.Queues[0].Type);
        Assert.True(options.Topology.Queues[0].DeadLetter);
        Assert.Equal("1000", options.Topology.Queues[0].Arguments["x-max-length"]);
        Assert.Equal("order.created", options.Topology.Bindings[0].RoutingKey);
    }

    [Fact]
    public void A_configure_callback_runs_after_the_configuration()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["acemq:url"] = "amqp://from-configuration:5672",
                ["acemq:format"] = "xml",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddAceMq(configure: o => o.Url = "amqp://from-code:5672");

        var options = services.BuildServiceProvider()
            .GetRequiredService<IOptions<AceMqOptions>>().Value;

        Assert.Equal("amqp://from-code:5672", options.Url);
        // Untouched by the callback, so still the configured value rather than the default.
        Assert.Equal("xml", options.Format);
    }

    [Fact]
    public void A_section_can_be_named_explicitly()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["messaging:url"] = "amqp://elsewhere:5672",
            })
            .Build();

        var services = new ServiceCollection();
        services.AddAceMq(configuration.GetSection("messaging"));

        var options = services.BuildServiceProvider()
            .GetRequiredService<IOptions<AceMqOptions>>().Value;

        Assert.Equal("amqp://elsewhere:5672", options.Url);
    }
}
