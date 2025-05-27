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

    public async Task AddOrder(OrderEntry order)
    {
        var orderBook = cache.GetOrAdd(order.ItemId, (key) =>
        {
            var book = new OrderBook();
            return book;
        });
        var side = orderBook.Sell;
        if (!order.IsSell)
            side = orderBook.Buy;

        var isOutBid = orderBook.TryGetOutbid(order, out var outbid);
        side.Add(order);

        if (isOutBid && outbid.UserId != null)
        {
            var kind = order.IsSell ? "sell" : "buy";
            var action = order.IsSell ? "undercut" : "outbid";
            var differencePrefix = order.IsSell ? "-" : "+";
            var names = await itemsApi.ItemNamesGetAsync();
            var name = names?.Where(n => n.Tag == order.ItemId).FirstOrDefault()?.Name;
            var differenceAmount = Math.Round(Math.Abs(outbid.PricePerUnit - order.PricePerUnit), 1);

            await messageApi.MessageSendUserIdPostAsync(outbid.UserId, new()
            {
                Summary = "You were " + action,
                //  by an order of 10000x at 8520.1 per unit (+101.5).
                Message = $"Your {kind}-order for {outbid.Amount}x {name ?? "item"} has been {action} by an order of {order.Amount}x at {Math.Round(order.PricePerUnit, 1)} per unit ({differencePrefix}{differenceAmount}).",
                Reference = (outbid.Amount + outbid.ItemId + outbid.PricePerUnit + outbid.Timestamp.Ticks).Truncate(32),
                SourceType = "bazaar",
                SourceSubId = "outbid"
            });
            logger.LogInformation($"order book: User {outbid.UserId} was {action} by {order.UserId} for {order.ItemId} {order.Amount}x {order.PricePerUnit}");
            // remove userId to prevent spamming
            outbid.UserId = null;
        }
        if (order.UserId != null)
        {// only save if it's a real user
            await InsertToDb(order);
            logger.LogInformation($"order book: User {order.UserId} added order for {order.ItemId} {order.Amount}x {order.PricePerUnit} {(order.IsSell ? "sell" : "buy")} at {order.Timestamp}");
        }
    }

    protected virtual async Task InsertToDb(OrderEntry order)
    {
        var insert = orderBookTable.Insert(order);
        insert.SetTTL(60 * 60 * 24 * 7);
        await insert.ExecuteAsync();
    }

    public async Task BazaarPull(BazaarPull pull)
    {
        await Parallel.ForEachAsync(pull.Products, async (product, cancle) =>
        {
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
                var order = new OrderEntry()
                {
                    Amount = item.Amount - current,
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
                var order = new OrderEntry()
                {
                    Amount = item.Amount - current,
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
                logger.LogWarning($"order book: User {order.UserId} tried to remove non existing order for {order.ItemId} {order.Amount}x {order.PricePerUnit} {order.Timestamp}");
            await RemoveFromDb(order);
        }
    }

    protected virtual async Task RemoveFromDb(OrderEntry item)
    {
        if (item.UserId == null)
            return;
        await orderBookTable.Where(o => o.ItemId == item.ItemId && o.Timestamp == item.Timestamp && o.UserId == item.UserId).Delete().ExecuteAsync();
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
                return;
            }
            catch (System.Exception e)
            {
                logger.LogError(e, "loading order book");
                await Task.Delay(10_000 * (i + 1));
            }
    }
}
