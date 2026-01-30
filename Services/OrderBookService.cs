using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
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
    private readonly IMessageApi messageApi;
    private IItemsApi itemsApi;
    private Table<OrderEntry> orderBookTable;
    private ISessionContainer sessionContainer;
    private ILogger<OrderBookService> logger;
    private ConcurrentDictionary<string, OrderBook> cache = new ConcurrentDictionary<string, OrderBook>();
    private ConcurrentDictionary<string, DateTime> lastKafkaUpdateTime = new ConcurrentDictionary<string, DateTime>();

    public OrderBookService(ISessionContainer service, IMessageApi messageApi, IItemsApi itemsApi, ILogger<OrderBookService> logger)
    {
        sessionContainer = service;
        this.messageApi = messageApi;
        this.itemsApi = itemsApi;
        this.logger = logger;
    }

    internal Task<OrderBook> GetOrderBook(string itemTag)
    {
        return Task.FromResult(cache.GetValueOrDefault(itemTag, new OrderBook()));
    }

    /// <summary>
    /// Gets order books for multiple items at once
    /// </summary>
    /// <param name="itemTags">List of item tags to lookup</param>
    /// <returns>Dictionary mapping item tags to their order books</returns>
    public Task<Dictionary<string, OrderBook>> GetOrderBooks(List<string> itemTags)
    {
        var result = new Dictionary<string, OrderBook>();
        foreach (var itemTag in itemTags)
        {
            result[itemTag] = cache.GetValueOrDefault(itemTag, new OrderBook());
        }
        return Task.FromResult(result);
    }

    /// <summary>
    /// Updates the in-memory order book with external data.
    /// Validates timestamp against Kafka data and ignores if older or in the future.
    /// </summary>
    /// <param name="update">The order book update</param>
    /// <returns>True if the update was applied, false if it was ignored</returns>
    public async Task<bool> UpdateOrderBook(OrderBookUpdate update)
    {
        var now = DateTime.UtcNow;

        // Ignore if timestamp is in the future
        if (update.Timestamp > now)
        {
            logger.LogWarning($"Ignoring order book update for {update.ItemTag}: timestamp {update.Timestamp} is in the future (now: {now})");
            return false;
        }

        // Check if we have a last Kafka update time for this item
        if (lastKafkaUpdateTime.TryGetValue(update.ItemTag, out var lastKafkaTime))
        {
            // Ignore if older than the last Kafka update
            if (update.Timestamp <= lastKafkaTime)
            {
                logger.LogWarning($"Ignoring order book update for {update.ItemTag}: timestamp {update.Timestamp} is older than last Kafka update {lastKafkaTime}");
                return false;
            }
        }

        var orderBook = cache.GetOrAdd(update.ItemTag, (key) => new OrderBook());

        // Update buy orders if provided (top orders only for outbid/undercut detection)
        if (update.BuyOrders != null && update.BuyOrders.Count > 0)
        {
            // Sort incoming buy orders by price descending (highest first)
            var incomingBuyOrders = update.BuyOrders.OrderByDescending(o => o.PricePerUnit).ToList();

            // Find existing top buy order (highest price)
            var topBuyOrder = orderBook.Buy.OrderByDescending(o => o.PricePerUnit).FirstOrDefault();

            // Remove orders that are no longer at the top (undercut)
            if (topBuyOrder != null && incomingBuyOrders.FirstOrDefault() != null)
            {
                var incomingTopPrice = incomingBuyOrders.First().PricePerUnit;
                if (Math.Round(topBuyOrder.PricePerUnit, 1) < Math.Round(incomingTopPrice, 1) && topBuyOrder.UserId != null)
                {
                    // Our top buy order was undercut
                    orderBook.Buy.Remove(topBuyOrder);
                    logger.LogInformation($"Removed extra buy order for {update.ItemTag} at price {topBuyOrder.PricePerUnit} - new:{incomingTopPrice}");

                    // Notify user about undercut
                    await SendUndercutNotification(topBuyOrder, incomingBuyOrders.First());
                }
            }

            // Update or add incoming top buy orders
            foreach (var incomingOrder in incomingBuyOrders)
            {
                incomingOrder.ItemId = update.ItemTag;
                incomingOrder.IsSell = false;
                incomingOrder.Timestamp = update.Timestamp;

                // Check if this price level already exists
                var existingOrder = orderBook.Buy.FirstOrDefault(o => Math.Round(o.PricePerUnit, 1) == Math.Round(incomingOrder.PricePerUnit, 1));
                if (existingOrder != null)
                {
                    // Update amount if it changed
                    existingOrder.Amount = incomingOrder.Amount;
                    // Remove order if it was filled (amount <= 0)
                    if (existingOrder.Amount <= 0)
                    {
                        orderBook.Buy.Remove(existingOrder);
                        logger.LogInformation($"Removed filled buy order for {update.ItemTag} at price {existingOrder.PricePerUnit} (amount was {incomingOrder.Amount})");
                    }
                }
                else
                {
                    // Only add new order if amount is positive
                    if (incomingOrder.Amount > 0)
                    {
                        orderBook.Buy.Add(incomingOrder);
                    }
                    else
                    {
                        logger.LogWarning($"Skipping invalid buy order for {update.ItemTag} at price {incomingOrder.PricePerUnit} with amount {incomingOrder.Amount}");
                    }
                }
            }

            // Remove buy orders that are covered by the incoming update range and are not present
            // Only remove orders within [minIncoming, maxIncoming] that aren't in the update
            // This handles filled orders while preserving orders outside the update range
            if (incomingBuyOrders.Count > 0)
            {
                var minIncomingPrice = incomingBuyOrders.Min(o => o.PricePerUnit);
                var maxIncomingPrice = incomingBuyOrders.Max(o => o.PricePerUnit);
                var incomingBuyPrices = incomingBuyOrders.Select(o => Math.Round(o.PricePerUnit, 1)).ToHashSet();
                var ordersToRemove = orderBook.Buy
                    .Where(o => o.PricePerUnit >= minIncomingPrice && o.PricePerUnit <= maxIncomingPrice
                        && !incomingBuyPrices.Contains(Math.Round(o.PricePerUnit, 1)))
                    .ToList();
                foreach (var order in ordersToRemove)
                {
                    orderBook.Buy.Remove(order);
                    logger.LogInformation($"Removed buy order for {update.ItemTag} at price {order.PricePerUnit} - not in incoming update (completely filled)");
                }
            }
        }

        // Update sell orders if provided (top orders only for outbid/undercut detection)
        if (update.SellOrders != null && update.SellOrders.Count > 0)
        {
            // Sort incoming sell orders by price ascending (lowest first)
            var incomingSellOrders = update.SellOrders.OrderBy(o => o.PricePerUnit).ToList();

            // Find existing top sell order (lowest price)
            var topSellOrder = orderBook.Sell.OrderBy(o => o.PricePerUnit).FirstOrDefault();

            // Remove orders that are no longer at the top (outbid)
            if (topSellOrder != null && incomingSellOrders.FirstOrDefault() != null)
            {
                var incomingTopPrice = incomingSellOrders.First().PricePerUnit;
                if (Math.Round(topSellOrder.PricePerUnit, 1) > Math.Round(incomingTopPrice, 1) && topSellOrder.UserId != null)
                {
                    // Our top sell order was outbid
                    orderBook.Sell.Remove(topSellOrder);
                    logger.LogInformation($"Removed outbid sell order for {update.ItemTag} at price {topSellOrder.PricePerUnit} - new:{incomingTopPrice}");

                    // Notify user about outbid
                    await SendOutbidNotification(incomingSellOrders.First(), topSellOrder);
                }
            }

            // Update or add incoming top sell orders
            foreach (var incomingOrder in incomingSellOrders)
            {
                incomingOrder.ItemId = update.ItemTag;
                incomingOrder.IsSell = true;
                incomingOrder.Timestamp = update.Timestamp;

                // Check if this price level already exists
                var existingOrder = orderBook.Sell.FirstOrDefault(o => Math.Round(o.PricePerUnit, 1) == Math.Round(incomingOrder.PricePerUnit, 1));
                if (existingOrder != null)
                {
                    // Update amount if it changed
                    existingOrder.Amount = incomingOrder.Amount;
                    // Remove order if it was filled (amount <= 0)
                    if (existingOrder.Amount <= 0)
                    {
                        orderBook.Sell.Remove(existingOrder);
                        logger.LogInformation($"Removed filled sell order for {update.ItemTag} at price {existingOrder.PricePerUnit} (amount was {incomingOrder.Amount})");
                    }
                }
                else
                {
                    // Only add new order if amount is positive
                    if (incomingOrder.Amount > 0)
                    {
                        orderBook.Sell.Add(incomingOrder);
                    }
                    else
                    {
                        logger.LogWarning($"Skipping invalid sell order for {update.ItemTag} at price {incomingOrder.PricePerUnit} with amount {incomingOrder.Amount}");
                    }
                }
            }

            // Remove sell orders that are covered by the incoming update range and are not present
            // Only remove orders within [minIncoming, maxIncoming] that aren't in the update
            // This handles filled orders while preserving orders outside the update range
            if (incomingSellOrders.Count > 0)
            {
                var minIncomingPrice = incomingSellOrders.Min(o => o.PricePerUnit);
                var maxIncomingPrice = incomingSellOrders.Max(o => o.PricePerUnit);
                var incomingSellPrices = incomingSellOrders.Select(o => Math.Round(o.PricePerUnit, 1)).ToHashSet();
                var sellOrdersToRemove = orderBook.Sell
                    .Where(o => o.PricePerUnit >= minIncomingPrice && o.PricePerUnit <= maxIncomingPrice
                        && !incomingSellPrices.Contains(Math.Round(o.PricePerUnit, 1)))
                    .ToList();
                foreach (var order in sellOrdersToRemove)
                {
                    orderBook.Sell.Remove(order);
                    logger.LogInformation($"Removed sell order for {update.ItemTag} at price {order.PricePerUnit} - not in incoming update (completely filled)");
                }
            }
        }

        logger.LogInformation($"Updated in-memory order book for {update.ItemTag} with timestamp {update.Timestamp} (buy: {update.BuyOrders?.Count ?? 0}, sell: {update.SellOrders?.Count ?? 0})");
        return true;
    }

    public async Task AddOrder(OrderEntry order)
    {
        // Reject orders with negative or zero amounts
        if (order.Amount <= 0)
        {
            logger.LogWarning($"order book: Rejecting order with invalid amount {order.Amount} for {order.ItemId} at {order.PricePerUnit}");
            return;
        }

        var orderBook = cache.GetOrAdd(order.ItemId, (key) =>
        {
            var book = new OrderBook();
            return book;
        });
        var side = orderBook.Sell;
        if (!order.IsSell)
            side = orderBook.Buy;

        // Get all orders that will be outbid by this new order
        var outbidOrders = orderBook.GetAllOutbidOrders(order);
        side.Add(order);

        // Notify all outbid users
        foreach (var outbid in outbidOrders)
        {
            await SendOutbidNotification(order, outbid);
            // Mark as notified to prevent duplicate notifications
            outbid.HasBeenNotified = true;
            await UpdateInDb(outbid);
        }

        if (order.UserId != null)
        {// only save if it's a real user
            await InsertToDb(order);
            logger.LogInformation($"order book: User {order.UserId} added order for {order.ItemId} {order.Amount}x {order.PricePerUnit} {(order.IsSell ? "sell" : "buy")} at {order.Timestamp}");
        }
    }

    private async Task SendOutbidNotification(OrderEntry newOrder, OrderEntry outbid)
    {
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

    private async Task SendUndercutNotification(OrderEntry existingOrder, OrderEntry undercuttingOrder)
    {
        var gray = "§7";
        var green = "§a";
        var red = "§c";
        var aqua = "§b";
        var names = await itemsApi.ItemNamesGetAsync();
        var name = names?.Where(n => n.Tag == existingOrder.ItemId).FirstOrDefault()?.Name;
        var differenceAmount = Math.Round(Math.Abs(existingOrder.PricePerUnit - undercuttingOrder.PricePerUnit), 1);

        await messageApi.MessageSendUserIdPostAsync(existingOrder.UserId, new()
        {
            Summary = "Your order was undercut",
            Message = $"{gray}Your {green}buy{gray}-order for {aqua}{existingOrder.Amount:N0}x {name ?? "item"}{gray} has been {red}undercut{gray} by an order of {aqua}{undercuttingOrder.Amount:N0}x{gray} "
             + $"at {green}{Math.Round(undercuttingOrder.PricePerUnit, 1):N1}{gray} per unit ({red}-{differenceAmount.ToString("N1")}{gray}).",
            Reference = $"{existingOrder.Amount:N0}{existingOrder.ItemId}{Math.Round(existingOrder.PricePerUnit, 1):N1}{existingOrder.Timestamp.Ticks}".Truncate(32),
            SourceType = "bazaar",
            SourceSubId = "undercut"
        });
        logger.LogInformation($"order book: User {existingOrder.UserId} was undercut for {existingOrder.ItemId} {existingOrder.Amount}x buy at {existingOrder.PricePerUnit}");
    }

    protected virtual async Task UpdateInDb(OrderEntry order)
    {
        if (order.UserId == null)
            return;
        // Update the order in the database to mark it as notified
        await orderBookTable.Where(o => o.ItemId == order.ItemId && o.Timestamp == order.Timestamp && o.UserId == order.UserId)
            .Select(o => new OrderEntry { HasBeenNotified = true })
            .Update()
            .ExecuteAsync();
    }

    protected virtual async Task InsertToDb(OrderEntry order)
    {
        var insert = orderBookTable.Insert(order);
        insert.SetTTL(60 * 60 * 24 * 7);
        await insert.ExecuteAsync();
    }

    public async Task BazaarPull(BazaarPull pull)
    {
        // Track the last update time from Kafka
        await Parallel.ForEachAsync(pull.Products, async (product, cancle) =>
        {
            lastKafkaUpdateTime.AddOrUpdate(product.ProductId, pull.Timestamp, (key, oldValue) =>
                pull.Timestamp > oldValue ? pull.Timestamp : oldValue);

            var orderBook = cache.GetOrAdd(product.ProductId, (key) =>
            {
                var book = new OrderBook();
                return book;
            });
            var currentMinSell = product.BuySummery.MinBy(o => o.PricePerUnit)?.PricePerUnit ?? 10_000_000;
            var currentMaxBuy = product.SellSummary.MaxBy(o => o.PricePerUnit)?.PricePerUnit ?? 0;
            await DropNotPresent(orderBook.Sell, (OrderEntry e) => e.PricePerUnit < currentMinSell && e.Timestamp < pull.Timestamp);
            await DropNotPresent(orderBook.Buy, (OrderEntry e) => e.PricePerUnit > currentMaxBuy && e.Timestamp < pull.Timestamp);
            var side = orderBook.Sell.ToList();
            // add/update new orders
            foreach (var item in product.BuySummery.OrderByDescending(o => o.PricePerUnit))
            {
                // find current
                var current = side.Where(o => o.PricePerUnit == item.PricePerUnit).Sum(o => o.Amount);
                if (current == item.Amount)
                    continue; // all orders known
                var delta = item.Amount - current;
                // Skip if delta is negative or zero (order was filled)
                if (delta <= 0)
                    continue;
                var order = new OrderEntry()
                {
                    Amount = delta,
                    IsSell = true,
                    ItemId = product.ProductId,
                    PlayerName = null,
                    PricePerUnit = item.PricePerUnit,
                    Timestamp = pull.Timestamp,
                    UserId = null
                };
                await AddOrder(order);
            }
            side = orderBook.Buy.ToList();
            foreach (var item in product.SellSummary.OrderBy(o => o.PricePerUnit))
            {
                // find current
                var current = side.Where(o => o.PricePerUnit == item.PricePerUnit).Sum(o => o.Amount);
                if (current == item.Amount)
                    continue; // all orders known
                var delta = item.Amount - current;
                // Skip if delta is negative or zero (order was filled)
                if (delta <= 0)
                    continue;
                var order = new OrderEntry()
                {
                    Amount = delta,
                    IsSell = false,
                    ItemId = product.ProductId,
                    PlayerName = null,
                    PricePerUnit = item.PricePerUnit,
                    Timestamp = pull.Timestamp,
                    UserId = null
                };
                await AddOrder(order);
            }
        });
    }

    private async Task DropNotPresent(List<OrderEntry> side, Func<OrderEntry, bool> missingFunc)
    {
        side.RemoveAll(o => o == null);
        // drop filled/canceled orders
        foreach (var item in side.Where(o => missingFunc(o) || o.Timestamp < DateTime.UtcNow - TimeSpan.FromDays(7)).ToList())
        {
            side.Remove(item);
            await RemoveFromDb(item);
        }
    }

    public async Task RemoveOrder(string itemTag, string userId, DateTime timestamp)
    {
        var orders = await orderBookTable.Where(o => o.ItemId == itemTag && o.Timestamp == timestamp && o.UserId == userId).ExecuteAsync();
        logger.LogInformation($"order book: User {userId} tries to remove order for {itemTag} {timestamp}");
        foreach (var order in orders)
        {
            var orderBook = cache.GetOrAdd(order.ItemId, (key) =>
            {
                var book = new OrderBook();
                return book;
            });
            if (orderBook.Remove(order))
                logger.LogInformation($"order book: User {order.UserId} removed order for {order.ItemId} {order.Amount}x {order.PricePerUnit}");
            else
            {
                logger.LogWarning($"order book: User {order.UserId} tried to remove non existing order for {order.ItemId} {order.Amount}x {order.PricePerUnit} {order.Timestamp}");
                if (int.TryParse(order.UserId, out int userIdInt) && userIdInt < 500)
                    logger.LogInformation($"{Newtonsoft.Json.JsonConvert.SerializeObject(orderBook)}\n{Newtonsoft.Json.JsonConvert.SerializeObject(order)}");
            }
            await RemoveFromDb(order);
        }
    }

    protected virtual async Task RemoveFromDb(OrderEntry item)
    {
        if (item.UserId == null)
            return;
        await orderBookTable.Where(o => o.ItemId == item.ItemId && o.Timestamp == item.Timestamp && o.UserId == item.UserId).Delete().ExecuteAsync();
    }

    /// <summary>
    /// Marks old orders as notified on restart to prevent spam.
    /// Only the top order (best price) per item can still be notified.
    /// </summary>
    private void MarkOldOrdersAsNotified()
    {
        foreach (var orderBook in cache.Values)
        {
            // For sell orders, keep only the lowest price order unmarked
            if (orderBook.Sell.Any(o => o.UserId != null))
            {
                var topSellOrder = orderBook.Sell
                    .Where(o => o.UserId != null)
                    .OrderBy(o => o.PricePerUnit)
                    .FirstOrDefault();

                foreach (var order in orderBook.Sell.Where(o => o.UserId != null && o != topSellOrder))
                {
                    order.HasBeenNotified = true;
                }
            }

            // For buy orders, keep only the highest price order unmarked
            if (orderBook.Buy.Any(o => o.UserId != null))
            {
                var topBuyOrder = orderBook.Buy
                    .Where(o => o.UserId != null)
                    .OrderByDescending(o => o.PricePerUnit)
                    .FirstOrDefault();

                foreach (var order in orderBook.Buy.Where(o => o.UserId != null && o != topBuyOrder))
                {
                    order.HasBeenNotified = true;
                }
            }
        }
    }

    internal async Task Load()
    {
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
            );
        ArgumentNullException.ThrowIfNull(sessionContainer.Session);
        orderBookTable = new Table<OrderEntry>(sessionContainer.Session, mapping);
        for (int i = 0; i < 100; i++)
            try
            {

                await orderBookTable.CreateIfNotExistsAsync();
                var orders = await orderBookTable.Select(o => o).ExecuteAsync();
                foreach (var order in orders)
                {
                    var orderBook = cache.GetOrAdd(order.ItemId, (key) =>
                    {
                        var book = new OrderBook();
                        return book;
                    });
                    var side = orderBook.Sell;
                    if (!order.IsSell)
                        side = orderBook.Buy;

                    side.Add(order);
                }

                // After loading all orders, mark old orders as notified except for the top order
                MarkOldOrdersAsNotified();

                return;
            }
            catch (System.Exception e)
            {
                logger.LogError(e, "loading order book");
                await Task.Delay(10_000 * (i + 1));
            }
    }
}
