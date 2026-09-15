using System;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Coflnet.Sky.Core;
using Coflnet.Sky.EventBroker.Client.Api;
using Coflnet.Sky.Items.Client.Api;
using Coflnet.Sky.SkyBazaar.Models;
using dev;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class OrderBookService
{
    private const int OrderBookLoadPageSize = 256;
    private readonly IMessageApi messageApi;
    private IItemsApi itemsApi;
    private Table<OrderEntry> orderBookTable;
    private ISessionContainer sessionContainer;
    private ILogger<OrderBookService> logger;
    private ConcurrentDictionary<string, OrderBook> cache = new ConcurrentDictionary<string, OrderBook>();
    private ConcurrentDictionary<string, DateTime> lastKafkaUpdateTime = new ConcurrentDictionary<string, DateTime>();
    private HashSet<string> lastBazaarItems = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> itemLocks = new();
    private readonly ConcurrentDictionary<string, OrderEntry> userOrders = new();
    private readonly BazaarOrderPublisher publisher;
    private volatile bool ordersLoaded;
    public virtual bool IsReady => ordersLoaded;
    private readonly ConcurrentDictionary<(string Item, string User, DateTime Time), byte> removedDuringLoad = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> playerLocks = new();
    private readonly ConcurrentDictionary<string, DateTime> playerObservationTimes = new();
    private static DateTime DatabaseTime(DateTime time) =>
        new(time.ToUniversalTime().Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond, DateTimeKind.Utc);
    internal List<OrderEntry> GetUserOrders(string userId) => userOrders.Values.Where(o => o.UserId == userId).Select(o => o.Copy()).ToList();

    internal Task<string> GetSnapshot(string userId) =>
        publisher.Publish(userId, null, false, () => GetUserOrders(userId), notify: false);

    internal static string OrderId(OrderEntry o) => $"{o.UserId}:{o.ItemId}:{o.Timestamp.Ticks}:{o.IsSell}";

    protected virtual Task PublishOrders(OrderEntry changed, bool created = false) =>
        publisher.Publish(changed.UserId, changed.PlayerName, created,
            () => GetUserOrders(changed.UserId));

    public OrderBookService(ISessionContainer service, IMessageApi messageApi, IItemsApi itemsApi, ILogger<OrderBookService> logger, BazaarOrderPublisher publisher)
    {
        sessionContainer = service;
        this.messageApi = messageApi;
        this.itemsApi = itemsApi;
        this.logger = logger;
        this.publisher = publisher;
    }

    internal async Task<OrderBook> GetOrderBook(string itemTag)
    {
        var gate = itemLocks.GetOrAdd(itemTag, _ => new(1));
        await gate.WaitAsync();
        try
        {
            var book = cache.GetValueOrDefault(itemTag, new());
            return new OrderBook { Buy = book.Buy.Select(o => o.Copy()).ToList(),
                Sell = book.Sell.Select(o => o.Copy()).ToList() };
        }
        finally { gate.Release(); }
    }

    /// <summary>
    /// Gets order books for multiple items at once
    /// </summary>
    /// <param name="itemTags">List of item tags to lookup</param>
    /// <returns>Dictionary mapping item tags to their order books</returns>
    public async Task<Dictionary<string, OrderBook>> GetOrderBooks(List<string> itemTags)
    {
        var result = new Dictionary<string, OrderBook>();
        foreach (var itemTag in itemTags)
        {
            result[itemTag] = await GetOrderBook(itemTag);
        }
        return result;
    }

    /// <summary>
    /// Updates the in-memory order book with external data.
    /// Validates timestamp against Kafka data and ignores if older or in the future.
    /// </summary>
    /// <param name="update">The order book update</param>
    /// <returns>True if the update was applied, false if it was ignored</returns>
    public async Task<bool> UpdateOrderBook(OrderBookUpdate update)
    {
        using var span = BazaarTelemetry.Source.StartActivity("bazaar.observe.price");
        span?.SetTag("bazaar.item_tag", update.ItemTag);
        span?.SetTag("bazaar.observed_at", update.Timestamp.ToString("O"));
        if (!IsReady || update.Timestamp < DateTime.UtcNow.AddSeconds(-10))
        {
            BazaarTelemetry.Observation(logger, "direct", update.ItemTag, update.Timestamp, !IsReady ? "loading" : "expired");
            return false;
        }
        var gate = itemLocks.GetOrAdd(update.ItemTag, _ => new(1));
        await gate.WaitAsync();
        try
        {
            var now = DateTime.UtcNow;
            var reason = update.Timestamp < now.AddSeconds(-10) ? "expired"
                : update.Timestamp > now ? "future"
                : lastKafkaUpdateTime.TryGetValue(update.ItemTag, out var last) && update.Timestamp <= last ? "out_of_order" : null;
            if (reason != null)
            {
                BazaarTelemetry.Observation(logger, "direct", update.ItemTag, update.Timestamp, reason);
                return false;
            }
            await ApplySide(update.ItemTag, update.BuyOrders, false, update.Timestamp, false);
            await ApplySide(update.ItemTag, update.SellOrders, true, update.Timestamp, false);
            lastKafkaUpdateTime[update.ItemTag] = update.Timestamp;
            BazaarTelemetry.Observation(logger, "direct", update.ItemTag, update.Timestamp, "applied");
            return true;
        }
        finally { gate.Release(); }
    }

    // Public observations are aggregate price levels. Match decreases against the FIFO queue;
    // never replace a player's original order amount with the aggregate amount at that price.
    private async Task ApplySide(string tag, List<OrderEntry> incoming, bool isSell, DateTime timestamp, bool fullSummary)
    {
        if (incoming == null || incoming.Count == 0 && !fullSummary)
            return;
        var book = cache.GetOrAdd(tag, _ => new());
        var side = isSell ? book.Sell : book.Buy;
        side.RemoveAll(o => o.UserId == null && o.Timestamp < DateTime.UtcNow.AddDays(-7));
        var levels = incoming.GroupBy(o => Math.Round(o.PricePerUnit, 1))
            .ToDictionary(g => g.Key, g => Math.Max(0, g.Sum(o => o.Amount)));
        var min = levels.Keys.DefaultIfEmpty(double.MaxValue).Min();
        var max = levels.Keys.DefaultIfEmpty(double.MinValue).Max();
        var positivePrices = levels.Where(p => p.Value > 0).Select(p => p.Key).ToList();
        bool PricePassed(double price) => positivePrices.Count > 0
            && (isSell ? price < positivePrices.Min() : price > positivePrices.Max());
        var covered = side.Where(o => o.Timestamp <= timestamp).Select(o => Math.Round(o.PricePerUnit, 1))
            .Where(price => fullSummary ? (isSell ? price <= max : price >= min) || levels.Count == 0
                : price >= min && price <= max || PricePassed(price));
        foreach (var price in levels.Keys.Union(covered).ToList())
        {
            var queue = side.Where(o => Math.Round(o.PricePerUnit, 1) == price && o.Timestamp <= timestamp)
                .OrderBy(o => o.Timestamp).ToList();
            var delta = levels.GetValueOrDefault(price) - queue.Sum(o => o.Amount - o.Filled);
            if (delta != 0 && logger.IsEnabled(LogLevel.Trace))
                logger.LogTrace("Bazaar level {ItemTag} sell {IsSell}, price {Price}: observed {Quantity}, remaining change {Delta}, queue entries {QueueCount}, at {ObservedAt:o}; TraceId {TraceId}",
                    tag, isSell, price, levels.GetValueOrDefault(price), delta, queue.Count, timestamp, Activity.Current?.TraceId.ToString());
            if (delta > 0)
            {
                // Preserve the queue position of older liquidity when new orders arrive.
                await AddOrderInternal(new OrderEntry { ItemId = tag, IsSell = isSell,
                    PricePerUnit = price, Amount = delta, Timestamp = timestamp });
            }
            else if (delta < 0)
            {
                foreach (var order in queue)
                {
                    var filled = Math.Min(-delta, order.Amount - order.Filled);
                    if (filled == 0)
                        continue;
                    delta += filled;
                    if (logger.IsEnabled(LogLevel.Trace))
                        logger.LogTrace("Bazaar FIFO allocation {OrderId}: {Allocation}, remaining decrease {RemainingDecrease}, at {ObservedAt:o}; TraceId {TraceId}",
                            OrderId(order), filled, -delta, timestamp, Activity.Current?.TraceId.ToString());
                    if (order.UserId == null)
                    {
                        order.Amount -= filled;
                        if (order.Amount == 0)
                            side.Remove(order);
                    }
                    else
                    {
                        var before = order.Filled;
                        var previousEstimate = order.IsEstimate;
                        order.Filled += filled;
                        order.IsEstimate = true;
                        if (order.Filled == order.Amount)
                            side.Remove(order);
                        await UpdateInDb(order);
                        BazaarTelemetry.Transition(logger, order, before, previousEstimate, "market_decrease", timestamp);
                        await PublishOrders(order);
                    }
                    if (delta == 0)
                        break;
                }
            }
        }
        foreach (var order in userOrders.Values.Where(o => o.ItemId == tag && o.IsSell == isSell
            && o.Timestamp <= timestamp && (o.Filled < o.Amount || o.IsEstimate != false)
            && PricePassed(Math.Round(o.PricePerUnit, 1))).ToList())
        {
            var before = order.Filled;
            var previousEstimate = order.IsEstimate;
            order.Filled = order.Amount;
            order.IsEstimate = false;
            side.Remove(order);
            await UpdateInDb(order);
            BazaarTelemetry.Transition(logger, order, before, previousEstimate, "price_passed", timestamp);
            await PublishOrders(order);
        }
    }

    public async Task AddOrder(OrderEntry order, bool observed = false, DateTime? observedAt = null)
    {
        var gate = itemLocks.GetOrAdd(order.ItemId, _ => new(1));
        await gate.WaitAsync();
        try { await AddOrderInternal(order, observed, observedAt); }
        finally { gate.Release(); }
    }

    private async Task AddOrderInternal(OrderEntry order, bool observed = false, DateTime? observedAt = null)
    {
        if (order.Amount <= 0)
            return;
        order.Timestamp = DatabaseTime(order.Timestamp);
        order.Filled = Math.Clamp(order.Filled, 0, order.Amount);
        order.IsEstimate = false; // Chat and personal order views are direct observations.
        var book = cache.GetOrAdd(order.ItemId, _ => new());
        var side = order.IsSell ? book.Sell : book.Buy;
        var created = false;
        var before = 0;
        bool? previousEstimate = null;
        if (order.UserId != null)
        {
            created = !userOrders.TryGetValue(OrderId(order), out var existing);
            if (existing != null)
            {
                before = existing.Filled;
                previousEstimate = existing.IsEstimate;
                side.Remove(existing);
                if ((!observed && order.Filled < existing.Filled)
                    || (observedAt.HasValue && lastKafkaUpdateTime.TryGetValue(order.ItemId, out var marketTime)
                        && observedAt.Value < marketTime))
                {
                    logger.LogDebug("Preserving newer fill for order {OrderId}; incoming {IncomingFilled}, current {Filled}, observation {ObservedAt}; TraceId {TraceId}",
                        OrderId(order), order.Filled, existing.Filled, observedAt, Activity.Current?.TraceId.ToString());
                    order.Filled = existing.Filled;
                    order.IsEstimate = existing.IsEstimate;
                }
                order.HasBeenNotified = existing.HasBeenNotified;
            }
            else
            {
                ReplaceAnonymous(side, order);
            }
            userOrders[OrderId(order)] = order;
        }
        var outbidOrders = order.Filled < order.Amount ? book.GetAllOutbidOrders(order) : new List<OrderEntry>();
        if (order.Filled < order.Amount)
            side.Add(order);
        if (order.UserId != null)
        {
            await InsertToDb(order);
            if (created || before != order.Filled || previousEstimate != order.IsEstimate)
                BazaarTelemetry.Transition(logger, order, before, previousEstimate, created ? "registered" : observed ? "personal_view" : "chat", observedAt ?? DateTime.UtcNow);
            await PublishOrders(order, created && !observed && order.Filled == 0);
        }
        foreach (var outbid in outbidOrders)
        {
            await SendOutbidNotification(order, outbid);
            outbid.HasBeenNotified = true;
            await UpdateInDb(outbid);
        }
    }

    private static void ReplaceAnonymous(List<OrderEntry> side, OrderEntry order)
    {
        // A late chat/description or startup load can identify liquidity already in the book.
        var remaining = order.Amount - order.Filled;
        foreach (var anonymous in side.Where(o => o.UserId == null && o.Timestamp >= order.Timestamp
            && Math.Round(o.PricePerUnit, 1) == Math.Round(order.PricePerUnit, 1)).ToList())
        {
            var replaced = Math.Min(anonymous.Amount, remaining);
            anonymous.Amount -= replaced;
            remaining -= replaced;
            if (anonymous.Amount == 0)
                side.Remove(anonymous);
            if (remaining == 0)
                break;
        }
    }

    private async Task SendOutbidNotification(OrderEntry newOrder, OrderEntry outbid)
    {
        // Skip notification if the outbid order doesn't have a valid UserId
        if (string.IsNullOrEmpty(outbid.UserId))
        {
            logger.LogWarning($"order book: Skipping outbid notification - outbid order has no UserId for {newOrder.ItemId}");
            return;
        }

        var gray = "§7";
        var green = "§a";
        var red = "§c";
        var aqua = "§b";
        var kind = newOrder.IsSell ? "sell" : "buy";
        var action = newOrder.IsSell ? "undercut" : "outbid";
        var differencePrefix = newOrder.IsSell ? "-" : "+";
        var names = await itemsApi.ItemNamesGetAsync();
        var name = names?.Where(n => n.Tag == newOrder.ItemId).FirstOrDefault()?.Name;
        var differenceAmount = Math.Round(Math.Abs(outbid.PricePerUnit - newOrder.PricePerUnit), 1);

        var undercutBySelf = outbid.UserId == newOrder.UserId;
        await messageApi.MessageSendUserIdPostAsync(outbid.UserId, new()
        {
            Summary = $"You were {action}",
            Message = $"{gray}Your {green}{kind}{gray}-order for {aqua}{outbid.Amount:N0}x {name ?? "item"}{gray} has been {red}{action}{gray} by an order of {aqua}{newOrder.Amount:N0}x{gray} "
             + $"at {green}{Math.Round(newOrder.PricePerUnit, 1):N1}{gray} per unit ({differencePrefix}{differenceAmount.ToString("N1")}).{(undercutBySelf ? " You undercut your own order!" : string.Empty)}",
            Reference = $"{outbid.Amount:N0}{outbid.ItemId}{Math.Round(outbid.PricePerUnit, 1):N1}{outbid.Timestamp.Ticks}".Truncate(32),
            SourceType = "bazaar",
            SourceSubId = "outbid"
        });
        logger.LogInformation($"order book: User {outbid.UserId} was {action} by {newOrder.UserId} for {newOrder.ItemId} {newOrder.Amount}x {newOrder.PricePerUnit}");
    }

    protected virtual async Task UpdateInDb(OrderEntry order)
    {
        if (order.UserId == null)
            return;
        await Persist(order, "update", () => orderBookTable.Where(o => o.ItemId == order.ItemId && o.Timestamp == order.Timestamp && o.UserId == order.UserId)
            .Select(o => new OrderEntry { HasBeenNotified = order.HasBeenNotified, Filled = order.Filled, IsEstimate = order.IsEstimate })
            .Update().ExecuteAsync());
    }

    protected virtual async Task InsertToDb(OrderEntry order)
    {
        var insert = orderBookTable.Insert(order);
        insert.SetTTL(60 * 60 * 24 * 7);
        await Persist(order, "insert", () => insert.ExecuteAsync());
    }

    public async Task BazaarPull(BazaarPull pull)
    {
        using var span = BazaarTelemetry.Source.StartActivity("bazaar.observe.market", ActivityKind.Consumer);
        span?.SetTag("bazaar.observed_at", pull.Timestamp.ToString("O"));
        if (!IsReady)
        {
            BazaarTelemetry.Observation(logger, "kafka", null, pull.Timestamp, "loading");
            return;
        }
        // Collect all item tags currently in this bazaar pull
        var currentBazaarItems = new HashSet<string>(pull.Products.Select(p => p.ProductId));

        await Parallel.ForEachAsync(pull.Products, async (product, cancellation) =>
        {
            var gate = itemLocks.GetOrAdd(product.ProductId, _ => new(1));
            await gate.WaitAsync(cancellation);
            try
            {
                if (lastKafkaUpdateTime.TryGetValue(product.ProductId, out var last) && pull.Timestamp <= last)
                {
                    BazaarTelemetry.Observation(logger, "kafka", product.ProductId, pull.Timestamp, "out_of_order");
                    return;
                }
                await ApplySide(product.ProductId, product.BuySummery.Select(o => new OrderEntry
                    { PricePerUnit = o.PricePerUnit, Amount = o.Amount }).ToList(), true, pull.Timestamp, true);
                await ApplySide(product.ProductId, product.SellSummary.Select(o => new OrderEntry
                    { PricePerUnit = o.PricePerUnit, Amount = o.Amount }).ToList(), false, pull.Timestamp, true);
                lastKafkaUpdateTime[product.ProductId] = pull.Timestamp;
                BazaarTelemetry.Observation(logger, "kafka", product.ProductId, pull.Timestamp, "applied");
            }
            finally { gate.Release(); }
        });

        var itemsToRemove = lastBazaarItems.Except(currentBazaarItems).ToHashSet();
        foreach (var order in userOrders.Values.Where(o => itemsToRemove.Contains(o.ItemId)
            || o.Timestamp < DateTime.UtcNow.AddDays(-7)).ToList())
            await RemoveOrder(order.ItemId, order.UserId, order.Timestamp);
        foreach (var itemTag in itemsToRemove)
            cache.TryRemove(itemTag, out _);

        // Update last bazaar items for next pull
        lastBazaarItems = currentBazaarItems;
    }

    public async Task MarkOrderFilled(string itemTag, string userId, double pricePerUnit, int amount)
    {
        var gate = itemLocks.GetOrAdd(itemTag, _ => new(1));
        await gate.WaitAsync();
        try
        {
            foreach (var order in userOrders.Values.Where(o => o.ItemId == itemTag && o.UserId == userId
                && Math.Round(o.PricePerUnit, 1) == Math.Round(pricePerUnit, 1) && o.Amount == amount).ToList())
            {
                var before = order.Filled;
                var previousEstimate = order.IsEstimate;
                order.Filled = order.Amount;
                order.IsEstimate = false;
                cache[itemTag].Remove(order);
                await UpdateInDb(order);
                if (before != order.Filled || previousEstimate != false)
                    BazaarTelemetry.Transition(logger, order, before, previousEstimate, "legacy_confirmed", DateTime.UtcNow);
                await PublishOrders(order);
            }
        }
        finally { gate.Release(); }
    }

    public async Task RemoveOrder(string itemTag, string userId, DateTime timestamp)
    {
        timestamp = DatabaseTime(timestamp);
        var gate = itemLocks.GetOrAdd(itemTag, _ => new(1));
        await gate.WaitAsync();
        try
        {
            if (!ordersLoaded)
                removedDuringLoad[(itemTag, userId, timestamp)] = 0;
            // Also delete orders not loaded into memory yet.
            await RemoveFromDb(new OrderEntry { ItemId = itemTag, UserId = userId, Timestamp = timestamp });
            foreach (var order in userOrders.Values.Where(o => o.ItemId == itemTag
                && o.UserId == userId && o.Timestamp == timestamp).ToList())
            {
                userOrders.TryRemove(OrderId(order), out _);
                BazaarTelemetry.Transition(logger, order, order.Filled, order.IsEstimate, "removed", DateTime.UtcNow);
                if (cache.TryGetValue(itemTag, out var book))
                    book.Remove(order);
                await PublishOrders(order);
            }
        }
        finally { gate.Release(); }
    }

    public async Task ObservePlayerOrders(PlayerOrderObservation observation)
    {
        using var span = BazaarTelemetry.Source.StartActivity("bazaar.observe.player");
        span?.SetTag("bazaar.user_id", observation.UserId);
        span?.SetTag("bazaar.observed_at", observation.Timestamp.ToString("O"));
        var player = $"{observation.UserId}:{observation.PlayerName}";
        var gate = playerLocks.GetOrAdd(player, _ => new(1));
        await gate.WaitAsync();
        try
        {
            if (playerObservationTimes.TryGetValue(player, out var last) && observation.Timestamp <= last)
            {
                logger.LogDebug("Ignored stale personal view for {UserId}/{PlayerName}: {ObservedAt:o} <= {LatestObservation:o}; TraceId {TraceId}",
                    observation.UserId, observation.PlayerName, observation.Timestamp, last, Activity.Current?.TraceId.ToString());
                return;
            }
            // Ownership comes from authenticated player state, never from item lore.
            foreach (var order in observation.Orders)
            {
                order.UserId = observation.UserId;
                order.PlayerName = observation.PlayerName;
                await AddOrder(order, observed: true, observedAt: observation.Timestamp);
            }
            var observed = observation.Orders.Select(OrderId).ToHashSet();
            foreach (var order in userOrders.Values.Where(o => o.UserId == observation.UserId
                && o.PlayerName == observation.PlayerName && o.Timestamp <= observation.Timestamp
                && !observed.Contains(OrderId(o))).ToList())
                await RemoveOrder(order.ItemId, order.UserId, order.Timestamp);
            playerObservationTimes[player] = observation.Timestamp;
            logger.LogDebug("Reconciled {OrderCount} orders for {UserId}/{PlayerName} at {ObservedAt:o}; TraceId {TraceId}",
                observation.Orders.Count, observation.UserId, observation.PlayerName, observation.Timestamp, Activity.Current?.TraceId.ToString());
        }
        finally { gate.Release(); }
    }

    protected virtual async Task RemoveFromDb(OrderEntry item)
    {
        if (item.UserId == null)
            return;
        await Persist(item, "delete", () => orderBookTable.Where(o => o.ItemId == item.ItemId && o.Timestamp == item.Timestamp && o.UserId == item.UserId).Delete().ExecuteAsync());
    }

    private async Task Persist(OrderEntry order, string operation, Func<Task> write)
    {
        using var span = BazaarTelemetry.Source.StartActivity("bazaar.persist");
        span?.SetTag("bazaar.order_id", OrderId(order));
        span?.SetTag("db.operation.name", operation);
        try { await write(); }
        catch (Exception e)
        {
            BazaarTelemetry.PersistenceFailures.WithLabels(operation).Inc();
            span?.SetStatus(ActivityStatusCode.Error, e.GetType().Name);
            logger.LogError(e, "Bazaar ledger {Operation} failed for {OrderId}; TraceId {TraceId}",
                operation, OrderId(order), Activity.Current?.TraceId.ToString());
            throw;
        }
    }

    /// <summary>
    /// Marks old orders as notified on restart to prevent spam.
    /// Only the top order (best price) per item can still be notified.
    /// </summary>
    private async Task MarkOldOrdersAsNotified()
    {
        foreach (var entry in cache)
        {
            var gate = itemLocks.GetOrAdd(entry.Key, _ => new(1));
            await gate.WaitAsync();
            try
            {
                foreach (var side in new[] { entry.Value.Buy, entry.Value.Sell })
                {
                    var tracked = side.Where(o => o.UserId != null)
                        .OrderBy(o => o.IsSell ? o.PricePerUnit : -o.PricePerUnit);
                    foreach (var order in tracked.Skip(1))
                        order.HasBeenNotified = true;
                }
            }
            finally { gate.Release(); }
        }
    }

    internal async Task AddLoadedOrder(OrderEntry order)
    {
        order.Timestamp = DatabaseTime(order.Timestamp);
        var gate = itemLocks.GetOrAdd(order.ItemId, _ => new(1));
        await gate.WaitAsync();
        try
        {
            if (removedDuringLoad.ContainsKey((order.ItemId, order.UserId, order.Timestamp))
                || !userOrders.TryAdd(OrderId(order), order))
                return; // A live observation/removal takes precedence over a startup database read.
            var book = cache.GetOrAdd(order.ItemId, _ => new());
            var side = order.IsSell ? book.Sell : book.Buy;
            ReplaceAnonymous(side, order);
            if (order.Filled < order.Amount)
                side.Add(order);
        }
        finally { gate.Release(); }
    }

    private async Task<int> LoadPersistedOrders()
    {
        var loadedOrders = 0;
        byte[] pagingState = null;

        do
        {
            var query = orderBookTable.Select(order => order);
            query.SetPageSize(OrderBookLoadPageSize);
            query.SetAutoPage(false);
            query.SetConsistencyLevel(ConsistencyLevel.LocalOne);
            if (pagingState != null && pagingState.Length != 0)
                query.SetPagingState(pagingState);

            var page = await query.ExecutePagedAsync().ConfigureAwait(false);
            foreach (var order in page)
            {
                await AddLoadedOrder(order);
                loadedOrders++;
            }

            pagingState = page.PagingState;
        }
        while (pagingState != null && pagingState.Length != 0);

        return loadedOrders;
    }

    internal async Task Load()
    {
        var started = Stopwatch.GetTimestamp();
        BazaarTelemetry.Ready.Set(0);
        logger.LogInformation("Loading Bazaar order ledger; matching unavailable until restore completes");
        var mapping = new MappingConfiguration()
            .Define(new Map<OrderEntry>()
                .PartitionKey(o => o.ItemId)
                .ClusteringKey(o => o.Timestamp)
                .ClusteringKey(o => o.UserId)
                .TableName("order_book")
                .Column(o => o.Amount, cm => cm.WithName("amount"))
                .Column(o => o.IsSell, cm => cm.WithName("is_sell"))
                .Column(o => o.PlayerName, cm => cm.WithName("player_name").WithSecondaryIndex())
                .Column(o => o.PricePerUnit, cm => cm.WithName("price_per_unit"))
                .Column(o => o.Timestamp, cm => cm.WithName("timestamp"))
                .Column(o => o.UserId, cm => cm.WithName("user_id").WithSecondaryIndex())
                .Column(o => o.ItemId, cm => cm.WithName("item_id"))
                .Column(o => o.HasBeenNotified, cm => cm.WithName("has_been_notified"))
                .Column(o => o.Filled, cm => cm.WithName("filled"))
                .Column(o => o.IsEstimate, cm => cm.WithName("is_estimate"))
            );
        ArgumentNullException.ThrowIfNull(sessionContainer.Session);
        orderBookTable = new Table<OrderEntry>(sessionContainer.Session, mapping);
        for (int i = 0; i < 100; i++)
            try
            {

                await orderBookTable.CreateIfNotExistsAsync();
                await Migrations.AddHasBeenNotifiedMigration.EnsureColumns(sessionContainer.Session, logger);
                var loadedOrders = await LoadPersistedOrders().ConfigureAwait(false);
                ordersLoaded = true;
                BazaarTelemetry.Ready.Set(1);
                removedDuringLoad.Clear();
                logger.LogInformation("Bazaar matching ready: loaded {OrderCount} entries for {ItemCount} items in {ElapsedMs} ms; initial publication follows", loadedOrders, cache.Count, Stopwatch.GetElapsedTime(started).TotalMilliseconds);

                // After loading all orders, mark old orders as notified except for the top order
                await MarkOldOrdersAsNotified();
                foreach (var order in userOrders.Values.DistinctBy(o => o.UserId))
                    await PublishOrders(order);

                return;
            }
            catch (System.Exception e)
            {
                // Cap delay at 60 s to avoid blocking for hours on repeated Cassandra timeouts
                var delayMs = Math.Min(10_000 * (i + 1), 60_000);
                logger.LogError(e, "loading order book (attempt {Attempt}/100, retrying in {Delay}s)", i + 1, delayMs / 1000);
                await Task.Delay(delayMs);
            }
    }
}
