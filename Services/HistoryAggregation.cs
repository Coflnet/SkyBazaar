using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.SkyBazaar.Models;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

/// <summary>
/// Zero-aware aggregation shared by the live 2-hour/daily jobs and the daily rebuild.
/// A price of 0 means "no orders on that side at that moment" and must never win a minimum
/// or drag a representative price down.
/// </summary>
internal static class HistoryAggregation
{
    /// <summary>The buy price the API shows for this row: first order of the book, else the stored price.</summary>
    public static double BuyPriceOf(StorageQuickStatus row)
        => row.BuyOrders?.FirstOrDefault()?.PricePerUnit ?? row.BuyPrice;

    /// <summary>The sell price the API shows for this row: first order of the book, else the stored price.</summary>
    public static double SellPriceOf(StorageQuickStatus row)
        => row.SellOrders?.FirstOrDefault()?.PricePerUnit ?? row.SellPrice;

    /// <summary>Smallest strictly positive value, 0 if there is none.</summary>
    public static double MinPositive(IEnumerable<double> values)
    {
        var min = 0d;
        foreach (var v in values.Where(v => v > 0))
            min = min == 0 ? v : Math.Min(min, v);
        return min;
    }

    /// <summary>Biggest strictly positive value, 0 if there is none.</summary>
    public static double MaxPositive(IEnumerable<double> values)
        => values.Where(v => v > 0).DefaultIfEmpty(0).Max();

    /// <summary>Price of the earliest child whose price is positive, 0 if none (the period's opening price, which is what candles draw).</summary>
    public static double OpeningPrice(IEnumerable<AggregatedQuickStatus> childrenByTime, Func<StorageQuickStatus, double> priceOf)
    {
        foreach (var child in childrenByTime)
        {
            var price = priceOf(child);
            if (price > 0)
                return price;
        }
        return 0;
    }

    /// <summary>
    /// Aggregates child rows (5-minute or 2-hour) into one row. Price = opening price (price of the earliest child with a positive price for that side),
    /// Min/Max ignore zeros, Count is summed. Volumes, moving week and order counts are copied from the first child
    /// (a snapshot, as before). No order books are carried, otherwise the API would show the first child's book
    /// instead of the representative price.
    /// </summary>
    public static AggregatedQuickStatus Aggregate(IReadOnlyList<AggregatedQuickStatus> children, DateTime timestamp)
    {
        if (children == null || children.Count == 0)
            return null;
        var ordered = children.OrderBy(c => c.TimeStamp).ToList();
        var buyPrices = ordered.Select(BuyPriceOf).ToList();
        var sellPrices = ordered.Select(SellPriceOf).ToList();
        var result = new AggregatedQuickStatus(ordered[0])
        {
            TimeStamp = timestamp,
            SerialisedBuyOrders = null,
            SerialisedSellOrders = null,
            BuyPrice = OpeningPrice(ordered, BuyPriceOf),
            SellPrice = OpeningPrice(ordered, SellPriceOf),
            MaxBuy = (float)MaxPositive(children.Select(c => (double)c.MaxBuy).Concat(buyPrices)),
            MaxSell = (float)MaxPositive(children.Select(c => (double)c.MaxSell).Concat(sellPrices)),
            MinBuy = (float)MinPositive(children.Select(c => (double)c.MinBuy).Concat(buyPrices)),
            MinSell = (float)MinPositive(children.Select(c => (double)c.MinSell).Concat(sellPrices)),
            Count = (short)Math.Min(short.MaxValue, children.Sum(c => (int)c.Count)),
        };
        return result;
    }
}
