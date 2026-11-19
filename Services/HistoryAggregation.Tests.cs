using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.SkyBazaar.Models;
using NUnit.Framework;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class HistoryAggregationTests
{
    private static readonly DateTime Day = new(2024, 11, 5, 0, 0, 0, DateTimeKind.Utc);

    private static AggregatedQuickStatus Child(double buy, double sell, float minBuy = 0, float minSell = 0, float maxBuy = 0, float maxSell = 0, short count = 30, int minute = 0)
        => new()
        {
            ProductId = "HARD_STONE",
            TimeStamp = Day.AddMinutes(minute),
            BuyPrice = buy,
            SellPrice = sell,
            MinBuy = minBuy,
            MinSell = minSell,
            MaxBuy = maxBuy,
            MaxSell = maxSell,
            Count = count,
            BuyVolume = 7,
        };

    [Test]
    public void ZerosAreIgnoredForMinAndPrice()
    {
        var result = HistoryAggregation.Aggregate(new[] { Child(10, 0), Child(12, 5), Child(0, 7) }, Day);
        Assert.That(result.MinBuy, Is.EqualTo(10f));
        Assert.That(result.MinSell, Is.EqualTo(5f));
        Assert.That(result.BuyPrice, Is.EqualTo(10)); // first positive buy
        Assert.That(result.SellPrice, Is.EqualTo(5)); // leading zero skipped
    }

    [Test]
    public void AllZeroGivesZero()
    {
        var result = HistoryAggregation.Aggregate(new[] { Child(0, 0), Child(0, 0) }, Day);
        Assert.That(new double[] { result.BuyPrice, result.SellPrice, result.MinBuy, result.MinSell, result.MaxBuy, result.MaxSell }, Is.All.EqualTo(0));
    }

    [Test]
    public void PriceIsTheEarliestPositive_EvenIfLaterOnesAreLowerOrHigher()
    {
        var low = HistoryAggregation.Aggregate(new[] { Child(10, 10, minute: 0), Child(5, 5, minute: 5), Child(50, 50, minute: 10) }, Day);
        Assert.That(low.BuyPrice, Is.EqualTo(10));
        Assert.That(low.SellPrice, Is.EqualTo(10));
    }

    [Test]
    public void LeadingZeroChildIsSkipped()
    {
        var result = HistoryAggregation.Aggregate(new[] { Child(0, 0, minute: 0), Child(7, 0, minute: 5), Child(9, 8, minute: 10) }, Day);
        Assert.That(result.BuyPrice, Is.EqualTo(7));
        Assert.That(result.SellPrice, Is.EqualTo(8));
    }

    [Test]
    public void UnsortedInputIsOrderedByTimestamp()
    {
        var result = HistoryAggregation.Aggregate(new[] { Child(30, 30, minute: 10), Child(10, 10, minute: 0), Child(20, 20, minute: 5) }, Day);
        Assert.That(result.BuyPrice, Is.EqualTo(10));
        Assert.That(result.SellPrice, Is.EqualTo(10));
    }

    [Test]
    public void OrderBookPricePerChildWinsOverStoredPrice()
    {
        var child = Child(buy: 99, sell: 98);
        child.BuyOrders = new List<Order> { new BuyOrder { PricePerUnit = 20, Amount = 1, Orders = 1 } };
        var result = HistoryAggregation.Aggregate(new[] { child }, Day);
        Assert.That(result.BuyPrice, Is.EqualTo(20));
        Assert.That(result.SellPrice, Is.EqualTo(98));
        Assert.That(result.MinBuy, Is.EqualTo(20f));
    }

    [Test]
    public void MinMaxIncludeChildExtremes()
    {
        var result = HistoryAggregation.Aggregate(new[] { Child(10, 10, minBuy: 4, maxBuy: 15), Child(11, 11, minBuy: 0, maxBuy: 12) }, Day);
        Assert.That(result.MinBuy, Is.EqualTo(4f));
        Assert.That(result.MaxBuy, Is.EqualTo(15f));
    }

    [Test]
    public void CountIsSummedAndVolumesComeFromFirstChild()
    {
        var first = Child(10, 10, count: 25);
        var second = Child(10, 10, count: 30, minute: 5);
        second.BuyVolume = 99;
        var result = HistoryAggregation.Aggregate(new[] { first, second }, Day);
        Assert.That(result.Count, Is.EqualTo(55));
        Assert.That(result.BuyVolume, Is.EqualTo(7));
    }

    [Test]
    public void ResultCarriesNoOrderBooksAndTheGivenTimestamp()
    {
        var child = Child(10, 10);
        child.BuyOrders = new List<Order> { new BuyOrder { PricePerUnit = 1 } };
        child.SellOrders = new List<Order> { new SellOrder { PricePerUnit = 1 } };
        var result = HistoryAggregation.Aggregate(new[] { child }, Day);
        Assert.That(result.SerialisedBuyOrders, Is.Null);
        Assert.That(result.SerialisedSellOrders, Is.Null);
        Assert.That(result.TimeStamp, Is.EqualTo(Day));
        Assert.That(result.BuyPrice, Is.EqualTo(1)); // from the book, not the first-child copy left on the result
    }

    [Test]
    public void DayWithZeroSellSamples_YieldsNonZeroMinSellAndSellPrice()
    {
        // regression: Min() over children made MinSell 0 and the first child made SellPrice 0
        var children = Enumerable.Range(0, 288)
            .Select(i => Child(buy: 100 + i % 3, sell: i % 4 == 0 ? 0 : 90 + i % 5, minSell: i % 4 == 0 ? 0 : 88, minute: i * 5))
            .ToList();
        var result = HistoryAggregation.Aggregate(children, Day);
        Assert.That(children[0].SellPrice, Is.EqualTo(0));
        Assert.That(result.MinSell, Is.GreaterThan(0));
        Assert.That(result.SellPrice, Is.GreaterThan(0));
        Assert.That(result.MinBuy, Is.GreaterThan(0));
    }
}
