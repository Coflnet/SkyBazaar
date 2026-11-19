using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.SkyBazaar.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using NUnit.Framework;
using StackExchange.Redis;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class DailyHistoryRebuildServiceTests
{
    private const string Product = "TEST_PRODUCT";
    private static readonly DateTime Start = new(2024, 11, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly int Bucket = ArchivedMinuteBlock.GetBucketId(Start);
    private static readonly DateTime BucketStart = ArchivedMinuteBlock.BucketStart(Bucket);

    private class FakeRebuild : DailyHistoryRebuildService
    {
        public readonly List<string> Events;
        public List<AggregatedQuickStatus> Hot = new();
        public List<AggregatedQuickStatus> Cold = new();
        public List<AggregatedQuickStatus> Legacy = new();
        public List<AggregatedQuickStatus> ExistingDaily = new();
        public readonly List<AggregatedQuickStatus> Inserted = new();
        public readonly List<DateTime> Deleted = new();
        public Exception LoadThrows;
        public Exception ColdThrows;
        public int OtherProductLoads;

        public FakeRebuild(IConnectionMultiplexer redis, List<string> events)
            : base(new BazaarService(new ConfigurationBuilder().Build(), NullLogger<BazaarService>.Instance, Mock.Of<Cassandra.ISession>()),
                Mock.Of<IBlobHistoryStore>(), redis, new ConfigurationBuilder().Build(), NullLogger<DailyHistoryRebuildService>.Instance)
        {
            Events = events;
        }

        private static List<AggregatedQuickStatus> InRange(List<AggregatedQuickStatus> rows, DateTime start, DateTime end)
            => rows.Where(r => r.TimeStamp >= start && r.TimeStamp < end).ToList();

        protected override Task<List<AggregatedQuickStatus>> LoadHotRowsAsync(string product, int bucket, DateTime start, DateTime end)
        {
            if (product != Product)
                OtherProductLoads++;
            if (LoadThrows != null)
                throw LoadThrows;
            return Task.FromResult(InRange(Hot, start, end));
        }

        protected override Task<List<AggregatedQuickStatus>> LoadLegacyRowsAsync(string product, DateTime start, DateTime end)
            => Task.FromResult(InRange(Legacy, start, end));

        protected override Task<List<AggregatedQuickStatus>> LoadColdRowsAsync(string product, int bucket, CancellationToken ct)
            => ColdThrows != null ? throw ColdThrows : Task.FromResult(InRange(Cold, ArchivedMinuteBlock.BucketStart(bucket), ArchivedMinuteBlock.BucketEnd(bucket)));

        protected override Task<List<AggregatedQuickStatus>> ReadDailyRowsAsync(string product, DateTime start, DateTime end)
            => Task.FromResult(ExistingDaily.Where(r => r.TimeStamp >= start && r.TimeStamp < end).ToList());

        protected override Task InsertDailyAsync(AggregatedQuickStatus row)
        {
            Events.Add("insert");
            Inserted.Add(row);
            return Task.CompletedTask;
        }

        protected override Task DeleteDailyAsync(string product, DateTime timeStamp)
        {
            Events.Add("delete");
            Deleted.Add(timeStamp);
            return Task.CompletedTask;
        }
    }

    private List<string> events;
    private Dictionary<string, string> markers;
    private Mock<IDatabase> db;
    private FakeRebuild service;

    [SetUp]
    public void Setup()
    {
        events = new List<string>();
        markers = new Dictionary<string, string>();
        db = new Mock<IDatabase>();
        db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .Returns<RedisKey, CommandFlags>((k, _) => Task.FromResult(markers.TryGetValue(k, out var v) ? (RedisValue)v : RedisValue.Null));
        db.Setup(d => d.StringSetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<Expiration>(), It.IsAny<ValueCondition>(), It.IsAny<CommandFlags>()))
            .Returns<RedisKey, RedisValue, Expiration, ValueCondition, CommandFlags>((k, v, _, _, _) =>
            {
                markers[k] = v;
                events.Add("marker");
                return Task.FromResult(true);
            });
        var redis = new Mock<IConnectionMultiplexer>();
        redis.Setup(r => r.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(db.Object);
        service = new FakeRebuild(redis.Object, events);
    }

    private static RebuildOptions Options(bool dry = false, DateTime? end = null)
        => new(dry, Start, end ?? Start.AddDays(40), 0);

    private static List<AggregatedQuickStatus> DayRows(DateTime day, int count, double sell = 10)
        => Enumerable.Range(0, count).Select(i => new AggregatedQuickStatus
        {
            ProductId = Product,
            TimeStamp = day.AddMinutes(i * 5),
            BuyPrice = 20,
            SellPrice = i % 2 == 0 ? 0 : sell,
            Count = 30,
        }).ToList();

    private Task<DailyHistoryRebuildService.RebuildSummary> Run(RebuildOptions o) => service.ProcessProductAsync(Product, o, db.Object, CancellationToken.None);

    [Test]
    public async Task GroupsByUtcDay_OneCanonicalRowPerDayAtMidnight()
    {
        service.Hot = DayRows(Start, 20).Concat(DayRows(Start.AddDays(1), 30)).ToList();
        await Run(Options());
        Assert.That(service.Inserted.Select(r => r.TimeStamp), Is.EqualTo(new[] { Start, Start.AddDays(1) }));
        Assert.That(service.Inserted[0].Count, Is.EqualTo(600));
        Assert.That(service.Inserted[0].SellPrice, Is.EqualTo(10));
        Assert.That(service.Inserted[0].MinSell, Is.EqualTo(10f));
    }

    [Test]
    public async Task DaysWithTooFewRows_AreSkippedAndExistingRowsStay()
    {
        service.Hot = DayRows(Start, DailyHistoryRebuildService.MinRowsPerDay - 1).Concat(DayRows(Start.AddDays(1), DailyHistoryRebuildService.MinRowsPerDay)).ToList();
        service.ExistingDaily = new() { new AggregatedQuickStatus { ProductId = Product, TimeStamp = Start.AddMinutes(3) } };
        await Run(Options());
        Assert.That(service.Inserted.Select(r => r.TimeStamp), Is.EqualTo(new[] { Start.AddDays(1) }));
        Assert.That(service.Deleted, Is.Empty);
    }

    [Test]
    public async Task DryRun_WritesNothingNotEvenTheMarker()
    {
        service.Hot = DayRows(Start, 20);
        service.ExistingDaily = new() { new AggregatedQuickStatus { ProductId = Product, TimeStamp = Start.AddMinutes(3) } };
        await Run(Options(dry: true));
        Assert.That(events, Is.Empty);
        Assert.That(markers, Is.Empty);
    }

    [Test]
    public async Task InsertsCanonicalRowBeforeDeletingOldRowsWithOtherTimestamps()
    {
        var old1 = new AggregatedQuickStatus { ProductId = Product, TimeStamp = Start.AddMinutes(3) };
        var old2 = new AggregatedQuickStatus { ProductId = Product, TimeStamp = Start.AddHours(1) };
        var same = new AggregatedQuickStatus { ProductId = Product, TimeStamp = Start };
        service.Hot = DayRows(Start, 20);
        service.ExistingDaily = new() { old1, same, old2 };
        await Run(Options());
        Assert.That(events.Take(3), Is.EqualTo(new[] { "insert", "delete", "delete" }));
        Assert.That(service.Deleted, Is.EquivalentTo(new[] { old1.TimeStamp, old2.TimeStamp }));
    }

    [Test]
    public async Task MergesAllTiers_ByFiveMinuteWindow()
    {
        service.Hot = DayRows(Start, 8);
        service.Cold = DayRows(Start, 8).Skip(4).Concat(DayRows(Start, 14).Skip(8)).ToList();
        service.Legacy = DayRows(Start, 20).Skip(14).ToList();
        await Run(Options());
        Assert.That(service.Inserted, Has.Count.EqualTo(1));
        Assert.That(service.Inserted[0].Count, Is.EqualTo(20 * 30));
    }

    [Test]
    public async Task MarkerWrittenAfterBucket_AndDoneBucketsAreSkipped()
    {
        service.Hot = DayRows(Start, 20);
        var o = Options(end: BucketStart.AddDays(30));
        await Run(o);
        Assert.That(markers[DailyHistoryRebuildService.MarkerPrefix + Product], Is.EqualTo(Bucket.ToString()));
        Assert.That(events.Last(), Is.EqualTo("marker"));
        service.Inserted.Clear();
        await Run(o);
        Assert.That(service.Inserted, Is.Empty);
    }

    [Test]
    public async Task FailureInABucket_LeavesNoMarker_AndRunMovesOnToNextProduct()
    {
        service.LoadThrows = new InvalidOperationException("boom");
        Assert.DoesNotThrowAsync(() => service.RunAsync(new[] { Product, "OTHER" }, Options(), db.Object, CancellationToken.None));
        Assert.That(markers, Is.Empty);
    }

    [Test]
    public async Task DaysOutsideStartAndEnd_AreLeftAlone()
    {
        service.Hot = DayRows(Start.AddDays(-1), 20).Concat(DayRows(Start, 20)).Concat(DayRows(Start.AddDays(3), 20)).ToList();
        await Run(Options(end: Start.AddDays(1)));
        Assert.That(service.Inserted.Select(r => r.TimeStamp), Is.EqualTo(new[] { Start }));
    }

    [Test]
    public async Task DryRun_ReadsExistingRowsAndCountsWhatWouldBeDeleted()
    {
        service.Hot = DayRows(Start, 20);
        service.ExistingDaily = new() { new AggregatedQuickStatus { ProductId = Product, TimeStamp = Start.AddMinutes(3) }, new AggregatedQuickStatus { ProductId = Product, TimeStamp = Start } };
        var summary = await Run(Options(dry: true));
        Assert.That(summary.Deleted, Is.EqualTo(1));
        Assert.That(summary.Written, Is.EqualTo(1));
        Assert.That(service.Deleted, Is.Empty);
        Assert.That(service.Inserted, Is.Empty);
    }

    [Test]
    public async Task TimeoutLikeCancellation_IsABucketFailure_NotAShutdown()
    {
        service.ColdThrows = new TaskCanceledException("R2 timeout");
        await service.RunAsync(new[] { Product, "OTHER" }, Options(), db.Object, CancellationToken.None);
        Assert.That(service.OtherProductLoads, Is.GreaterThan(0), "the run must continue with the next product");
        Assert.That(markers, Is.Empty);
    }

    [Test]
    public void RealShutdown_StillPropagates()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.ThrowsAsync<OperationCanceledException>(() => service.RunAsync(new[] { Product }, Options(), db.Object, cts.Token));
    }

    [Test]
    public async Task BucketWithoutAnyRows_DoesNotAdvanceTheMarker()
    {
        await Run(Options(end: BucketStart.AddDays(30)));
        Assert.That(markers, Is.Empty);
    }

    [Test]
    public void NonDryRun_IsRefusedWithoutTheColdTier()
    {
        // the mocked blob store reports IsEnabled=false
        Assert.That(service.CanRun(Options(dry: false)), Is.False);
        Assert.That(service.CanRun(Options(dry: true)), Is.True);
    }
}
