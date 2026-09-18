using AceMq.Amqp.Hosting;

namespace AceMq.Amqp.Hosting.Tests;

/// <summary>
/// Settings in, library types out. Pure functions, so tested directly.
/// </summary>
public class AceMqConnectionsTests
{
    [Fact]
    public void Config_carries_the_url_credentials_and_timeouts()
    {
        var config = AceMqConnections.ConfigFrom(new AceMqOptions
        {
            Url = "amqp://broker:5672",
            Username = "svc",
            Password = "hunter2",
            VirtualHost = "/orders",
            ClientName = "orders-worker",
            ConnectionTimeout = TimeSpan.FromSeconds(5),
            ConfirmTimeout = TimeSpan.FromSeconds(45),
            MaxOutstandingPublishes = 512,
        });

        Assert.Equal("amqp://broker:5672", config.Url);
        Assert.Equal("svc", config.Username);
        Assert.Equal("/orders", config.VirtualHost);
        Assert.Equal("orders-worker", config.ClientName);
        Assert.Equal(TimeSpan.FromSeconds(5), config.ConnectionTimeout);
        Assert.Equal(TimeSpan.FromSeconds(45), config.ConfirmTimeout);
        Assert.Equal(512, config.MaxOutstandingPublishes);
        Assert.True(config.PublisherConfirms);
    }

    [Fact]
    public void Publisher_confirms_can_be_turned_off_and_nothing_else_does_it()
    {
        Assert.False(AceMqConnections
            .ConfigFrom(new AceMqOptions { PublisherConfirms = false }).PublisherConfirms);
        Assert.True(AceMqConnections
            .ConfigFrom(new AceMqOptions()).PublisherConfirms);
    }

    [Fact]
    public void A_username_without_a_password_is_ignored_rather_than_sent_empty()
    {
        var config = AceMqConnections.ConfigFrom(new AceMqOptions
        {
            Url = "amqp://guest:guest@broker:5672",
            Username = "svc",
        });

        // The URL's credentials survive; the half-set ones do not overwrite them.
        Assert.NotEqual("svc", config.Username);
    }

    [Fact]
    public void Tls_is_absent_unless_asked_for()
    {
        Assert.Null(AceMqConnections.TlsFrom(new AceMqOptions { Url = "amqp://localhost:5672" }));
    }

    [Fact]
    public void An_amqps_url_turns_tls_on_even_when_the_mode_was_left_alone()
    {
        var tls = AceMqConnections.TlsFrom(new AceMqOptions { Url = "amqps://broker:5671" });

        Assert.NotNull(tls);
        Assert.Equal(TlsMode.Required, tls!.Mode);
    }

    [Fact]
    public void Insecure_is_only_ever_reached_by_asking_for_it_by_name()
    {
        var tls = AceMqConnections.TlsFrom(new AceMqOptions
        {
            Url = "amqps://broker:5671",
            Tls = { Mode = AceMqTlsMode.Insecure },
        });

        Assert.Equal(TlsMode.Insecure, tls!.Mode);
    }

    [Fact]
    public void Tls_details_reach_the_options()
    {
        var tls = AceMqConnections.TlsFrom(new AceMqOptions
        {
            Url = "amqps://broker:5671",
            Tls =
            {
                ServerName = "broker.internal",
                CheckRevocation = false,
                AllowDevelopmentCertificates = true,
            },
        });

        Assert.Equal("broker.internal", tls!.ServerName);
        Assert.False(tls.CheckRevocation);
        Assert.True(tls.DevelopmentCertificatesAllowed);
    }

    [Fact]
    public void The_codec_is_looked_up_by_name()
    {
        Assert.Equal("application/json",
            AceMqConnections.CodecFrom(new AceMqOptions { Format = "json" }).ContentType);
        Assert.Equal("application/json",
            AceMqConnections.CodecFrom(new AceMqOptions { Format = "" }).ContentType);
        Assert.Throws<AceFatalException>(
            () => AceMqConnections.CodecFrom(new AceMqOptions { Format = "no-such-codec" }));
    }

    [Fact]
    public void No_topology_is_declared_when_none_is_configured()
    {
        Assert.Null(AceMqConnections.TopologyFrom(new AceMqTopologyOptions()));
    }

    [Fact]
    public void Apply_none_means_the_declarations_are_documentation()
    {
        var topology = new AceMqTopologyOptions { Apply = TopologyApply.None };
        topology.Queues.Add(new AceMqTopologyOptions.QueueOptions { Name = "orders.new" });

        Assert.Null(AceMqConnections.TopologyFrom(topology));
    }

    [Fact]
    public void Topology_carries_exchanges_queues_and_bindings()
    {
        var options = new AceMqTopologyOptions();
        options.Exchanges.Add(new AceMqTopologyOptions.ExchangeOptions
        {
            Name = "orders",
            Type = "topic",
        });
        options.Queues.Add(new AceMqTopologyOptions.QueueOptions
        {
            Name = "orders.new",
            Type = QueueType.Quorum,
            Arguments = { ["x-max-length"] = "1000", ["x-single-active-consumer"] = "true" },
        });
        options.Bindings.Add(new AceMqTopologyOptions.BindingOptions
        {
            Queue = "orders.new",
            Exchange = "orders",
            RoutingKey = "order.created",
        });

        var topology = AceMqConnections.TopologyFrom(options)!;

        Assert.Equal("orders", topology.Exchanges[0].Name);
        Assert.Equal("orders.new", topology.Queues[0].Name);
        Assert.Equal(QueueType.Quorum, topology.Queues[0].Type);
        // Coerced: a broker rejects x-max-length as the string "1000".
        Assert.Equal(1000L, topology.Queues[0].Arguments["x-max-length"]);
        Assert.Equal(true, topology.Queues[0].Arguments["x-single-active-consumer"]);
        Assert.Equal("order.created", topology.Bindings[0].RoutingKey);
    }

    [Fact]
    public void A_dead_letter_queue_brings_its_dlq_with_it()
    {
        var options = new AceMqTopologyOptions();
        options.Queues.Add(new AceMqTopologyOptions.QueueOptions
        {
            Name = "orders.new",
            DeadLetter = true,
        });

        var topology = AceMqConnections.TopologyFrom(options)!;

        Assert.Contains(topology.Queues, q => q.Name == "orders.new.dlq");
    }

    [Fact]
    public void There_is_no_retry_policy_until_retries_are_enabled()
    {
        Assert.Null(AceMqConnections.RetryPolicyFrom(new AceMqRetryOptions()));
    }

    [Fact]
    public void The_retry_ladder_is_built_from_the_settings()
    {
        var policy = AceMqConnections.RetryPolicyFrom(new AceMqRetryOptions
        {
            Enabled = true,
            MaxAttempts = 4,
            InitialDelay = TimeSpan.FromSeconds(2),
            MaxDelay = TimeSpan.FromMinutes(2),
            Multiplier = 3,
            Jitter = 0.2,
            GiveUpAfter = TimeSpan.FromHours(1),
            BrokerWaitThreshold = TimeSpan.FromSeconds(10),
        })!;

        Assert.Equal(4, policy.MaxAttempts);
        Assert.Equal(TimeSpan.FromSeconds(2), policy.InitialDelay);
        Assert.Equal(TimeSpan.FromMinutes(2), policy.MaxDelay);
        Assert.Equal(3.0, policy.Multiplier);
        Assert.Equal(0.2, policy.JitterFactor);
        Assert.Equal(TimeSpan.FromHours(1), policy.MaxMessageAge);
        Assert.Equal(TimeSpan.FromSeconds(10), policy.BrokerWaitThreshold);
        // The thing the threshold is for: a delay at or over it waits on the broker, so a
        // drain does not have to sit through it.
        Assert.True(policy.WaitsInBroker(TimeSpan.FromSeconds(30)));
        Assert.False(policy.WaitsInBroker(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public void Consumer_options_take_the_listener_defaults()
    {
        var options = AceMqConnections.ConsumerOptionsFrom(
            new AceMqListenerOptions { Prefetch = 33, RequeueOnFailure = true },
            registration: null,
            codec: null);

        Assert.Equal(33, options.PrefetchCount);
        Assert.True(options.RequeueOnFailure);
        Assert.Null(options.RetryPolicy);
    }
}
