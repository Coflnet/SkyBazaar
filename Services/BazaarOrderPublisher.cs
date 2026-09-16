using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.Items.Client.Api;
using Coflnet.Sky.SkyBazaar.Models;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class BazaarOrderPublisher([FromKeyedServices("bazaar")] IConnectionMultiplexer redis, IItemsApi items, ILogger<BazaarOrderPublisher> logger) : BackgroundService
{
    public const string Channel = "bazaar:orders:v1";
    public const string FillStream = "bazaar:fills:v1";
    // Save the HUD/API state and enqueue each confirmed completion with deduplication. Delivery to
    // external notification targets happens in EventBroker, outside the price update request.
    // This is a rebuildable cache; the seven-day authoritative ledger lives in Scylla.
    // Retaining every inactive user's snapshot for seven days exhausted the shared Redis.
    internal const string PublishScript = """
        local enqueued = 0
        redis.call('SET', KEYS[1], ARGV[1], 'EX', 600)
        redis.call('PUBLISH', KEYS[2], ARGV[1])
        for i = 4, #KEYS do
            if redis.call('EXISTS', KEYS[i]) == 0 then
                redis.call('XADD', KEYS[3], '*', 'message', ARGV[i - 2])
                redis.call('SET', KEYS[i], '1', 'EX', 691200)
                enqueued = enqueued + 1
            end
        end
        return enqueued
        """;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> userLocks = new();
    private readonly ConcurrentDictionary<string, string> names = new();
    private int namesLoading;
    private long revision = DateTime.UtcNow.Ticks;
    private readonly ConcurrentDictionary<string, Pending> pending = new();
    private sealed class Pending
    {
        public string Payload;
        public long Revision;
        public ActivityContext Context;
        public int Failures;
        public readonly long Started = Stopwatch.GetTimestamp();
        public readonly Dictionary<string, string> Fills = new();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(10));
        while (await timer.WaitForNextTickAsync(stoppingToken))
            await RetryPending();
    }

    internal async Task RetryPending()
    {
        foreach (var userId in pending.Keys)
        {
            var gate = userLocks.GetOrAdd(userId, _ => new(1));
            await gate.WaitAsync();
            try
            {
                if (pending.TryGetValue(userId, out var update))
                    await TryPublish(userId, update);
            }
            finally { gate.Release(); }
        }
    }

    private async Task TryPublish(string userId, Pending update)
    {
        using var span = BazaarTelemetry.Source.StartActivity("bazaar.publish.attempt", ActivityKind.Producer, update.Context);
        span?.SetTag("bazaar.user_id", userId);
        span?.SetTag("bazaar.revision", update.Revision);
        var keys = new List<RedisKey> { $"{Channel}:{userId}", Channel, FillStream };
        keys.AddRange(update.Fills.Keys.Select(reference => (RedisKey)$"bazaar:filled:v1:{reference}"));
        var values = new List<RedisValue> { update.Payload };
        values.AddRange(update.Fills.Values.Select(message => (RedisValue)message));
        try
        {
            var result = await redis.GetDatabase().ScriptEvaluateAsync(PublishScript, keys.ToArray(), values.ToArray());
            BazaarTelemetry.Publications.WithLabels("success").Inc();
            var enqueued = result == null ? 0 : (long)result;
            if (enqueued > 0)
                logger.LogInformation("Enqueued {FillCount} confirmed Bazaar fills for {UserId}, revision {Revision}; TraceId {TraceId}",
                    enqueued, userId, update.Revision, Activity.Current?.TraceId.ToString());
            var level = update.Failures > 0 ? LogLevel.Information : LogLevel.Debug;
            if (logger.IsEnabled(level))
                logger.Log(level,
                "Published Bazaar snapshot for {UserId}, revision {Revision}, candidates {FillCandidates}, newly enqueued {Enqueued}, previous failures {Failures}, elapsed {ElapsedMs} ms; TraceId {TraceId}",
                userId, update.Revision, update.Fills.Count, enqueued, update.Failures, Stopwatch.GetElapsedTime(update.Started).TotalMilliseconds, Activity.Current?.TraceId.ToString());
            pending.TryRemove(userId, out _);
            BazaarTelemetry.PendingUsers.Dec();
            BazaarTelemetry.PendingFills.Dec(update.Fills.Count);
        }
        catch (RedisException e)
        {
            update.Failures++;
            BazaarTelemetry.Publications.WithLabels("failure").Inc();
            span?.SetStatus(ActivityStatusCode.Error, e.GetType().Name);
            logger.LogWarning(e, "Bazaar publication pending for {UserId}, revision {Revision}, fills {FillCount}, failures {Failures}, elapsed {ElapsedMs} ms; retry in 10 seconds; TraceId {TraceId}",
                userId, update.Revision, update.Fills.Count, update.Failures, Stopwatch.GetElapsedTime(update.Started).TotalMilliseconds, Activity.Current?.TraceId.ToString());
        }
    }

    private async Task RefreshNames()
    {
        if (Interlocked.CompareExchange(ref namesLoading, 1, 0) != 0)
            return;
        try
        {
            foreach (var item in await items.ItemNamesGetAsync())
                names[item.Tag] = item.Name;
        }
        catch (Exception e) { logger.LogWarning(e, "Using item tags for Bazaar order display"); }
        finally { Volatile.Write(ref namesLoading, 0); }
    }

    public async Task<string> Publish(string userId, string playerName, bool created, Func<List<OrderEntry>> getOrders, bool notify = true)
    {
        using var span = BazaarTelemetry.Source.StartActivity("bazaar.snapshot", ActivityKind.Producer);
        span?.SetTag("bazaar.user_id", userId);
        var gate = userLocks.GetOrAdd(userId, _ => new(1));
        await gate.WaitAsync();
        try
        {
            var orders = getOrders();
            if (orders.Any(o => !names.ContainsKey(o.ItemId)))
                _ = RefreshNames(); // Item-name lookup must not delay matching updates or alerts.
            if (!pending.TryGetValue(userId, out var update))
            {
                pending[userId] = update = new();
                BazaarTelemetry.PendingUsers.Inc();
            }
            update.Revision = Interlocked.Increment(ref revision);
            update.Context = Activity.Current?.Context ?? default;
            span?.SetTag("bazaar.revision", update.Revision);
            var payload = JsonConvert.SerializeObject(new {
                UserId = userId, PlayerName = playerName, Created = created,
                Revision = update.Revision, Orders = orders,
                TraceParent = Activity.Current?.Id, TraceState = Activity.Current?.TraceStateString,
                ItemNames = orders.Select(o => o.ItemId).Distinct()
                    .ToDictionary(tag => tag, tag => names.GetValueOrDefault(tag, tag))
            });
            update.Payload = payload;
            foreach (var order in orders.Where(o => notify && !o.IsExpired && o.Timestamp > DateTime.UtcNow.AddDays(-7) && o.Amount > 0 && o.Filled == o.Amount && o.IsEstimate == false))
            {
                var reference = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(
                    $"bazaar-filled:{userId}:{order.ItemId}:{order.IsSell}:{order.Timestamp.Ticks}")))[..32];
                if (update.Fills.ContainsKey(reference))
                    continue; // Keep the original event's trace and revision across publication retries.
                update.Fills[reference] = JsonConvert.SerializeObject(new {
                    OrderId = OrderBookService.OrderId(order), Revision = update.Revision,
                    TraceParent = Activity.Current?.Id, TraceState = Activity.Current?.TraceStateString,
                    Reference = reference, User = new { UserId = userId },
                    SourceType = "bazaar", SourceSubId = "filled", Summary = "Bazaar order filled",
                    Message = $"Your {(order.IsSell ? "sell offer" : "buy order")} for {order.Amount:N0}x {names.GetValueOrDefault(order.ItemId, order.ItemId)} was filled!",
                    Link = $"https://sky.coflnet.com/item/{Uri.EscapeDataString(order.ItemId)}",
                    Setings = new { StoreIfOffline = true }, Timestamp = DateTime.UtcNow,
                    Data = new { order.ItemId, order.PlayerName, order.Amount, order.Filled, order.IsSell, IsEstimate = false }
                });
                BazaarTelemetry.PendingFills.Inc();
                logger.LogDebug("Prepared confirmed Bazaar fill {Reference} for order {OrderId}, revision {Revision}; TraceId {TraceId}",
                    reference, OrderBookService.OrderId(order), update.Revision, Activity.Current?.TraceId.ToString());
            }
            await TryPublish(userId, update);
            return payload;
        }
        finally { gate.Release(); }
    }
}
