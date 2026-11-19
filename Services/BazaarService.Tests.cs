using System;
using System.Collections.Generic;
using System.Linq;
using Coflnet.Sky.SkyBazaar.Models;
using NUnit.Framework;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class BazaarServiceTests
{
    /// <summary>
    /// The live minute aggregation printed a line per product and window,
    /// which rotated the container log within half an hour
    /// </summary>
    [Test]
    public void LiveMinuteAggregationWindowIsNotLogged()
    {
        Assert.That(BazaarService.ShouldLogChecked(TimeSpan.FromMinutes(10)), Is.False);
        Assert.That(BazaarService.ShouldLogChecked(TimeSpan.FromMinutes(5)), Is.False);
    }

    [Test]
    public void HourAndBackfillWindowsAreLogged()
    {
        Assert.That(BazaarService.ShouldLogChecked(TimeSpan.FromHours(4)), Is.True);
        Assert.That(BazaarService.ShouldLogChecked(TimeSpan.FromHours(6)), Is.True);
        Assert.That(BazaarService.ShouldLogChecked(TimeSpan.FromDays(2)), Is.True);
    }

    /// <summary>
    /// v1 cold blobs truncated timestamps to whole seconds while hot rows have ms precision,
    /// so the same 5-minute row used to appear twice in the overlap
    /// </summary>
    [Test]
    public void MergeHistoryResults_CollapsesSubSecondDuplicates_KeepsExistingRow()
    {
        var t = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var hot = new AggregatedQuickStatus { ProductId = "P", TimeStamp = t.AddMilliseconds(437), BuyPrice = 1 };
        var cold = new AggregatedQuickStatus { ProductId = "P", TimeStamp = t, BuyPrice = 2 };
        var other = new AggregatedQuickStatus { ProductId = "P", TimeStamp = t.AddMinutes(5), BuyPrice = 3 };
        var results = new List<AggregatedQuickStatus> { hot };
        BazaarService.MergeHistoryResults(results, new[] { cold, other }, 10);
        Assert.That(results, Has.Count.EqualTo(2));
        Assert.That(results.Single(r => r.TimeStamp < t.AddMinutes(1)).BuyPrice, Is.EqualTo(1));
        Assert.That(results[0].TimeStamp, Is.GreaterThan(results[1].TimeStamp));
    }

    [Test]
    public void MergeHistoryResults_KeepsSerialisedOrderBooks()
    {
        var t = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var row = new AggregatedQuickStatus { ProductId = "P", TimeStamp = t, SerialisedBuyOrders = new byte[] { 1, 2, 3 }, SerialisedSellOrders = new byte[] { 4 } };
        var results = new List<AggregatedQuickStatus>();
        BazaarService.MergeHistoryResults(results, new[] { row }, 10);
        Assert.That(results.Single().SerialisedBuyOrders, Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(results.Single().SerialisedSellOrders, Is.EqualTo(new byte[] { 4 }));
    }

    [Test]
    public void LiveDayAggregationStart_IsUtcMidnightTwoDaysBack()
    {
        var now = new DateTime(2026, 6, 10, 2, 0, 17, DateTimeKind.Utc);
        Assert.That(BazaarService.LiveDayAggregationStart(now), Is.EqualTo(new DateTime(2026, 6, 8, 0, 0, 0, DateTimeKind.Utc)));
    }

    [Test]
    public void ShouldAggregateDays_OnlyJustAfterTwoAmUtc()
    {
        var day = new DateTime(2026, 6, 10, 0, 0, 0, DateTimeKind.Utc);
        Assert.That(BazaarService.ShouldAggregateDays(day.AddHours(2).AddSeconds(5)), Is.True);
        Assert.That(BazaarService.ShouldAggregateDays(day.AddHours(2).AddSeconds(30)), Is.False);
        Assert.That(BazaarService.ShouldAggregateDays(day.AddSeconds(5)), Is.False);
        Assert.That(BazaarService.ShouldAggregateDays(day.AddHours(4).AddSeconds(5)), Is.False);
    }

    [Test]
    public void LiveDayWindowAtTwoAm_CoversYesterday()
    {
        var now = new DateTime(2026, 6, 10, 2, 0, 30, DateTimeKind.Utc);
        var start = BazaarService.LiveDayAggregationStart(now);
        var length = TimeSpan.FromDays(2);
        Assert.That(start + length < now, Is.True, "the loop runs this window");
        Assert.That(start + length + length < now, Is.False, "and no further one");
        var yesterday = new DateTime(2026, 6, 9, 0, 0, 0, DateTimeKind.Utc);
        Assert.That(start <= yesterday && yesterday + TimeSpan.FromDays(1) <= start + length, Is.True);
    }

    [Test]
    public void GetTable_ShortRangeBeyondSecondsRetention_FallsBackToMinuteTier()
    {
        var now = new DateTime(2026, 6, 10, 12, 0, 0, DateTimeKind.Utc);
        var recent = BazaarService.GetTable(now.AddMinutes(-30), now, now);
        var old = BazaarService.GetTable(now.AddDays(-30), now.AddDays(-30).AddMinutes(30), now);
        Assert.That(recent, Is.EqualTo("QuickStatusSeconds"));
        Assert.That(old, Is.EqualTo("QuickStatusMin15Day"));
    }
}
