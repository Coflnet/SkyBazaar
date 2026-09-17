using System;
using System.Diagnostics;
using Coflnet.Sky.SkyBazaar.Models;
using Microsoft.Extensions.Logging;
using Prometheus;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

internal static class BazaarTelemetry
{
    internal const string SourceName = "Coflnet.Sky.Bazaar";
    internal static readonly ActivitySource Source = new(SourceName);
    internal static readonly Counter Observations = Metrics.CreateCounter("sky_bazaar_observations_total", "Market observations by source and outcome", new CounterConfiguration { LabelNames = new[] { "source", "result" } });
    internal static readonly Counter Transitions = Metrics.CreateCounter("sky_bazaar_order_transitions_total", "Owned-order changes by reason", new CounterConfiguration { LabelNames = new[] { "reason" } });
    internal static readonly Counter Publications = Metrics.CreateCounter("sky_bazaar_publications_total", "Snapshot publication attempts", new CounterConfiguration { LabelNames = new[] { "result" } });
    internal static readonly Gauge PendingUsers = Metrics.CreateGauge("sky_bazaar_pending_publication_users", "Users awaiting Redis publication in this process");
    internal static readonly Gauge PendingFills = Metrics.CreateGauge("sky_bazaar_pending_fill_events", "Confirmed fill events awaiting Redis publication in this process");
    internal static readonly Gauge Ready = Metrics.CreateGauge("sky_bazaar_matching_ready", "Whether the owned-order ledger has loaded");
    internal static readonly Counter PersistenceFailures = Metrics.CreateCounter("sky_bazaar_order_persistence_failures_total", "Failed order ledger writes", new CounterConfiguration { LabelNames = new[] { "operation" } });

    private static readonly double[] LatencyBuckets = { .01, .05, .1, .25, .5, 1, 2, 5, 10, 30, 60 };
    internal static readonly Histogram MatchDuration = Metrics.CreateHistogram("sky_bazaar_match_duration_seconds",
        "Successful matching through ledger persistence and snapshot publication attempts, including lock waits", new HistogramConfiguration { LabelNames = new[] { "source" }, Buckets = LatencyBuckets });
    internal static readonly Histogram ObservationAge = Metrics.CreateHistogram("sky_bazaar_matched_observation_age_seconds",
        "Source observation age after matching and publication; includes upstream transport delay", new HistogramConfiguration { LabelNames = new[] { "source" }, Buckets = LatencyBuckets });

    internal static void Matched(string source, long started, DateTime observedAt)
    {
        MatchDuration.WithLabels(source).Observe(Stopwatch.GetElapsedTime(started).TotalSeconds);
        ObservationAge.WithLabels(source).Observe(Math.Max(0, (DateTime.UtcNow - observedAt).TotalSeconds));
    }

    internal static void Observation(ILogger logger, string source, string itemTag, DateTime timestamp, string result)
    {
        Observations.WithLabels(source, result).Inc();
        if (logger.IsEnabled(LogLevel.Trace))
            logger.LogTrace("Bazaar observation {Source} {ItemTag} at {ObservedAt:o}: {Result}; TraceId {TraceId}",
            source, itemTag, timestamp, result, Activity.Current?.TraceId.ToString());
    }

    internal static void Transition(ILogger logger, OrderEntry order, int before, bool? previousEstimate, string reason, DateTime observedAt)
    {
        Transitions.WithLabels(reason).Inc();
        var level = reason == "market_decrease" ? LogLevel.Debug : LogLevel.Information;
        if (logger.IsEnabled(level))
            logger.Log(level, "Bazaar order {OrderId} {Reason}: filled {PreviousFilled}->{Filled}/{Amount}, estimate {PreviousEstimate}->{IsEstimate}, expired {IsExpired}, claimed {Claimed}, price {Price}, observation {ObservedAt:o}; TraceId {TraceId}",
            OrderBookService.OrderId(order), reason, before, order.Filled, order.Amount, previousEstimate, order.IsEstimate,
            order.IsExpired, order.Claimed, order.PricePerUnit, observedAt, Activity.Current?.TraceId.ToString());
        if (Activity.Current?.IsAllDataRequested == true)
            Activity.Current.AddEvent(new ActivityEvent("bazaar.order.changed", tags: new ActivityTagsCollection {
                { "bazaar.order_id", OrderBookService.OrderId(order) }, { "bazaar.reason", reason },
                { "bazaar.previous_filled", before }, { "bazaar.filled", order.Filled },
                { "bazaar.is_expired", order.IsExpired }, { "bazaar.claimed", order.Claimed },
                { "bazaar.is_estimate", order.IsEstimate }, { "bazaar.observed_at", observedAt.ToString("O") }
            }));
    }
}
