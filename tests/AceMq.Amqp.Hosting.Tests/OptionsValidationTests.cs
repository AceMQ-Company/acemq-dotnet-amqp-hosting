using AceMq.Amqp.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace AceMq.Amqp.Hosting.Tests;

/// <summary>
/// Configuration that cannot work is refused where the setting is named, not several
/// layers down where it is not.
/// </summary>
public class OptionsValidationTests
{
    private static string FailureFor(Action<AceMqOptions> configure)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddAceMq(configure: configure);

        var failure = Assert.Throws<OptionsValidationException>(
            () => services.BuildServiceProvider().GetRequiredService<IOptions<AceMqOptions>>().Value);
        return string.Join("; ", failure.Failures);
    }

    [Fact]
    public void A_url_with_no_scheme_is_refused()
    {
        // Uri.TryCreate calls this absolute, reading "broker" as the scheme. The scheme is
        // what picks the transport, so the validator has to be stricter than Uri is.
        Assert.Contains("acemq:url is not a broker URL", FailureFor(o => o.Url = "broker:5672"));
    }

    [Fact]
    public void An_empty_url_is_refused() =>
        Assert.Contains("acemq:url is required", FailureFor(o => o.Url = ""));

    [Fact]
    public void A_prefetch_of_zero_is_refused() =>
        Assert.Contains(
            "acemq:listener:prefetch must be at least 1",
            FailureFor(o => o.Listener.Prefetch = 0));

    [Fact]
    public void A_concurrency_of_zero_is_refused() =>
        Assert.Contains(
            "acemq:listener:concurrency must be at least 1",
            FailureFor(o => o.Listener.Concurrency = 0));

    [Fact]
    public void A_multiplier_below_one_is_refused_because_it_is_not_a_backoff() =>
        Assert.Contains(
            "which is the opposite of a backoff",
            FailureFor(o =>
            {
                o.Listener.Retry.Enabled = true;
                o.Listener.Retry.Multiplier = 0.5;
            }));

    [Fact]
    public void A_max_delay_below_the_initial_delay_is_refused() =>
        Assert.Contains(
            "maxDelay is shorter than the initial delay",
            FailureFor(o =>
            {
                o.Listener.Retry.Enabled = true;
                o.Listener.Retry.InitialDelay = TimeSpan.FromMinutes(5);
                o.Listener.Retry.MaxDelay = TimeSpan.FromSeconds(1);
            }));

    [Fact]
    public void Jitter_outside_zero_to_one_is_refused() =>
        Assert.Contains(
            "jitter is a fraction between 0 and 1",
            FailureFor(o =>
            {
                o.Listener.Retry.Enabled = true;
                o.Listener.Retry.Jitter = 4;
            }));

    [Fact]
    public void A_retry_ladder_that_is_off_is_not_validated()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddAceMq(configure: o => o.Listener.Retry.Multiplier = 0.5);

        // Nonsense, but unreachable nonsense: nothing reads it while Enabled is false.
        var options = services.BuildServiceProvider()
            .GetRequiredService<IOptions<AceMqOptions>>().Value;
        Assert.Equal(0.5, options.Listener.Retry.Multiplier);
    }

    [Fact]
    public void A_queue_with_no_name_is_refused() =>
        Assert.Contains(
            "every entry in acemq:topology:queues needs a name",
            FailureFor(o => o.Topology.Queues.Add(new AceMqTopologyOptions.QueueOptions())));

    [Fact]
    public void Nothing_is_validated_when_acemq_is_disabled()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
        services.AddAceMq(configure: o =>
        {
            o.Enabled = false;
            o.Url = "not a url";
            o.Listener.Prefetch = 0;
        });

        var options = services.BuildServiceProvider()
            .GetRequiredService<IOptions<AceMqOptions>>().Value;
        Assert.False(options.Enabled);
    }
}
