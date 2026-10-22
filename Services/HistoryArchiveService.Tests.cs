using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.SkyBazaar.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using NUnit.Framework;
using StackExchange.Redis;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class HistoryArchiveServiceTests
{
    private const string Product = "TEST_PRODUCT";
    private const int Bucket = 77;

    private enum ReadBack { Normal, Null, Mismatch }

    private class NoDbHistoryArchiveService : HistoryArchiveService
    {
        public readonly List<string> Events;
        public readonly Dictionary<int, List<AggregatedQuickStatus>> Hot = new();
        public readonly Dictionary<int, List<AggregatedQuickStatus>> Legacy = new();
        public int LegacyCalls;
        public Exception DeleteThrows;

        public NoDbHistoryArchiveService(IBlobHistoryStore store, IConnectionMultiplexer redis, List<string> events)
            : base(new BazaarService(new ConfigurationBuilder().Build(), NullLogger<BazaarService>.Instance, Mock.Of<Cassandra.ISession>()),
                store, redis, new ConfigurationBuilder().Build(), NullLogger<HistoryArchiveService>.Instance)
        {
            Events = events;
        }

        protected override Task<List<AggregatedQuickStatus>> LoadHotRowsAsync(string productId, int bucketId, DateTime start, DateTime end)
        {
            Events.Add("load_hot");
            return Task.FromResult(Hot.TryGetValue(bucketId, out var r) ? r.ToList() : new List<AggregatedQuickStatus>());
        }

        protected override Task<List<AggregatedQuickStatus>> LoadLegacyRowsAsync(string productId, DateTime bucketStart, DateTime bucketEnd)
        {
            LegacyCalls++;
            Events.Add("load_legacy");
            var bucketId = ArchivedMinuteBlock.GetBucketId(bucketStart);
            return Task.FromResult(Legacy.TryGetValue(bucketId, out var r) ? r.ToList() : new List<AggregatedQuickStatus>());
        }

        protected override Task DeleteHotPartitionsAsync(string productId, int bucketId, CancellationToken cancellationToken)
        {
            Events.Add("delete");
            if (DeleteThrows != null)
                throw DeleteThrows;
            Hot.Remove(bucketId);
            return Task.CompletedTask;
        }

        protected override IReadOnlyList<string> GetProducts() => new[] { Product };

        public Task<BucketOutcome> Process(int bucketId, DateTime now, SweepOptions o, IDatabase db)
            => ProcessBucketAsync(Product, bucketId, now, o, db, CancellationToken.None);
    }

    private List<string> events;
    private Dictionary<string, string> markers;
    private Dictionary<int, ArchivedMinuteBlock> blobs;
    private ReadBack readBack;
    private bool writeThrows;
    private Mock<IBlobHistoryStore> store;
    private Mock<IDatabase> db;
    private NoDbHistoryArchiveService service;

    private static DateTime EligibleNow(int bucket) => ArchivedMinuteBlock.BucketEnd(bucket).AddDays(70);
    private static DateTime TooEarlyNow(int bucket) => ArchivedMinuteBlock.BucketEnd(bucket).AddDays(10);

    private static SweepOptions Options(bool delete = true, bool legacyDropped = false, int maxDeletes = 0)
        => new SweepOptions(7, 30, 31, delete, legacyDropped, maxDeletes);

    [SetUp]
    public void Setup()
    {
        events = new List<string>();
        markers = new Dictionary<string, string>();
        blobs = new Dictionary<int, ArchivedMinuteBlock>();
        readBack = ReadBack.Normal;
        writeThrows = false;

        store = new Mock<IBlobHistoryStore>();
        store.SetupGet(s => s.IsEnabled).Returns(true);
        store.Setup(s => s.WriteBlobAsync(It.IsAny<ArchivedMinuteBlock>(), It.IsAny<CancellationToken>()))
            .Returns<ArchivedMinuteBlock, CancellationToken>((b, _) =>
            {
                events.Add("write");
                if (writeThrows)
                    throw new InvalidOperationException("write failed");
                blobs[b.BucketId] = RoundTrip(b);
                return Task.CompletedTask;
            });
        store.Setup(s => s.ReadBlobUncachedAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
            .Returns<string, int, CancellationToken>((p, b, _) =>
            {
                events.Add("read_uncached");
                if (readBack == ReadBack.Null || !blobs.TryGetValue(b, out var blob))
                    return Task.FromResult<ArchivedMinuteBlock>(null);
                var copy = RoundTrip(blob);
                if (readBack == ReadBack.Mismatch)
                    copy.SerialisedBuyOrders[0] = null;
                return Task.FromResult(copy);
            });

        db = new Mock<IDatabase>();
        db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .Returns<RedisKey, CommandFlags>((k, _) => Task.FromResult(markers.TryGetValue(k, out var v) ? (RedisValue)v : RedisValue.Null));
        db.Setup(d => d.KeyExistsAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .Returns<RedisKey, CommandFlags>((k, _) => Task.FromResult(markers.ContainsKey(k)));


        db.Setup(d => d.StringSetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<Expiration>(), It.IsAny<ValueCondition>(), It.IsAny<CommandFlags>()))
            .Returns<RedisKey, RedisValue, Expiration, ValueCondition, CommandFlags>((k, v, _, _, _) =>
            {
                markers[k] = v;
                events.Add("marker:" + HistoryArchiveServiceMarkerKind(v));
                return Task.FromResult(true);
            });

        var redis = new Mock<IConnectionMultiplexer>();
        redis.Setup(r => r.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(db.Object);
        service = new NoDbHistoryArchiveService(store.Object, redis.Object, events);
    }

    private static string HistoryArchiveServiceMarkerKind(string value) => value.Split(':')[0];

    private static ArchivedMinuteBlock RoundTrip(ArchivedMinuteBlock b)
        => MessagePack.MessagePackSerializer.Deserialize<ArchivedMinuteBlock>(MessagePack.MessagePackSerializer.Serialize(b));

    private static AggregatedQuickStatus Row(DateTime ts, short count = 5, bool books = true, long refId = 1, double price = 10)
    {
        var row = new AggregatedQuickStatus { ProductId = Product, TimeStamp = ts, Count = count, BuyPrice = price, SellPrice = price - 1, ReferenceId = refId };
        if (books)
        {
            row.BuyOrders = new[] { new BuyOrder { Amount = 3, PricePerUnit = price } };
            row.SellOrders = new[] { new SellOrder { Amount = 2, PricePerUnit = price - 1 } };
        }
        return row;
    }

    private static List<AggregatedQuickStatus> Rows(int bucket, int n = 4, bool books = true)
    {
        var start = ArchivedMinuteBlock.BucketStart(bucket).AddDays(2);
        return Enumerable.Range(0, n).Select(i => Row(start.AddMinutes(5 * i).AddMilliseconds(123), books: books, refId: i + 1)).ToList();
    }

    // ---- pure helpers ----

    [Test]
    public void NewestClosedBucket_ExcludesBucketStillOpen()
    {
        // 3 days after the start of bucket 80: bucket 80 is still open and 79 ended less than 7 days ago? no: 79 ended 3 days ago.
        var now = ArchivedMinuteBlock.BucketStart(80).AddDays(3);
        var newest = HistoryArchiveService.NewestClosedBucket(now, 7);
        Assert.That(newest, Is.EqualTo(78));
        Assert.That(ArchivedMinuteBlock.BucketEnd(newest), Is.LessThanOrEqualTo(now.AddDays(-7)));
        // exactly on the boundary the bucket that just ended counts
        Assert.That(HistoryArchiveService.NewestClosedBucket(ArchivedMinuteBlock.BucketEnd(79).AddDays(7), 7), Is.EqualTo(79));
        // the old computation returned the bucket containing now-7d, i.e. one that is still open
        Assert.That(ArchivedMinuteBlock.GetBucketId(now.AddDays(-7)), Is.EqualTo(79));
    }

    [Test]
    public void FifteenDayBucketIds_CoverExactlyTheThirtyDayWindow()
    {
        var (first, second) = ArchivedMinuteBlock.FifteenDayBucketIds(77);
        Assert.That((first, second), Is.EqualTo((154, 155)));
        Assert.That(FifteenDayAggregatedQuickStatus.GetBucketId(new DateTime(2026, 5, 14, 0, 0, 0, DateTimeKind.Utc)), Is.EqualTo(155));
        Assert.That(ArchivedMinuteBlock.GetBucketId(new DateTime(2026, 5, 14, 0, 0, 0, DateTimeKind.Utc)), Is.EqualTo(77));
        var start = ArchivedMinuteBlock.BucketStart(77);
        var end = ArchivedMinuteBlock.BucketEnd(77);
        Assert.That(FifteenDayAggregatedQuickStatus.GetBucketId(start), Is.EqualTo(first));
        Assert.That(FifteenDayAggregatedQuickStatus.GetBucketId(end.AddTicks(-1)), Is.EqualTo(second));
        Assert.That(FifteenDayAggregatedQuickStatus.GetBucketId(end), Is.EqualTo(second + 1));
    }

    [Test]
    public void MergeForArchive_DedupesByFiveMinuteWindow_PrefersHigherCountThenHot()
    {
        var t = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var hotLowCount = Row(t.AddSeconds(10), count: 2, price: 1);
        var legacyHighCount = Row(t.AddMinutes(1), count: 5, price: 2);
        var hotTie = Row(t.AddMinutes(5), count: 5, price: 3);
        var legacyTie = Row(t.AddMinutes(5).AddSeconds(30), count: 5, price: 4);
        var merged = HistoryArchiveService.MergeForArchive(new[] { hotLowCount, hotTie }, new[] { legacyHighCount, legacyTie });
        Assert.That(merged, Has.Count.EqualTo(2));
        Assert.That(merged[0].BuyPrice, Is.EqualTo(2));
        Assert.That(merged[1].BuyPrice, Is.EqualTo(3));
    }

    [Test]
    public void MergeForArchive_SortsAscending()
    {
        var t = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var hot = new[] { Row(t.AddMinutes(20)), Row(t) };
        var legacy = new[] { Row(t.AddMinutes(10)), Row(t.AddMinutes(30)) };
        var merged = HistoryArchiveService.MergeForArchive(hot, legacy);
        Assert.That(merged.Select(r => r.TimeStamp), Is.Ordered.Ascending);
        Assert.That(merged, Has.Count.EqualTo(4));
    }

    [Test]
    public void MergeForArchive_PrefersRowWithOrderBooks_OnEqualCount()
    {
        var t = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var hotNoBooks = Row(t, books: false, price: 1);
        var legacyBooks = Row(t.AddSeconds(5), books: true, price: 2);
        var merged = HistoryArchiveService.MergeForArchive(new[] { hotNoBooks }, new[] { legacyBooks });
        Assert.That(merged, Has.Count.EqualTo(1));
        Assert.That(merged[0].BuyPrice, Is.EqualTo(2));
        Assert.That(merged[0].SerialisedBuyOrders, Is.Not.Null);
    }

    [Test]
    public void IsDeleteEligible_FalseInsideHotWindowPlusMargin_TrueBeyond()
    {
        var o = Options();
        var end = ArchivedMinuteBlock.BucketEnd(Bucket);
        Assert.That(HistoryArchiveService.IsDeleteEligible(Bucket, end.AddDays(40), o), Is.False);
        Assert.That(HistoryArchiveService.IsDeleteEligible(Bucket, end.AddDays(70), o), Is.True);
        // larger margin pushes it out
        var wide = new SweepOptions(7, 30, 90, true, false, 0);
        Assert.That(HistoryArchiveService.IsDeleteEligible(Bucket, end.AddDays(70), wide), Is.False);
    }

    [Test]
    public void IsDeleteEligible_NeverBelowSecondsRetentionBound()
    {
        var o = new SweepOptions(7, 1, 0, true, false, 0);
        var end = ArchivedMinuteBlock.BucketEnd(Bucket);
        Assert.That(HistoryArchiveService.IsDeleteEligible(Bucket, end.AddDays(60), o), Is.False);
        Assert.That(HistoryArchiveService.IsDeleteEligible(Bucket, end.AddDays(61).AddMinutes(1), o), Is.True);
        Assert.That(BazaarService.SECONDS_RETENTION_SECONDS / 86400, Is.EqualTo(60));
    }

    // ---- ProcessBucket ----

    [Test]
    public async Task ProcessBucket_WritesVerifiesMarksThenDeletes_InThatOrder()
    {
        service.Hot[Bucket] = Rows(Bucket);
        var outcome = await service.Process(Bucket, EligibleNow(Bucket), Options(), db.Object);
        Assert.That(outcome, Is.EqualTo(BucketOutcome.Deleted));
        Assert.That(events, Is.EqualTo(new[]
        {
            "load_hot", "load_legacy", "read_uncached", "write", "read_uncached", "marker:archived", "delete", "marker:deleted"
        }));
        Assert.That(markers["bazaar_archive:v2:" + Product + ":" + Bucket], Does.StartWith("deleted:4:"));
    }

    [Test]
    public async Task ProcessBucket_NeverDeletes_WhenReadBackMissing()
    {
        service.Hot[Bucket] = Rows(Bucket);
        readBack = ReadBack.Null;
        var outcome = await service.Process(Bucket, EligibleNow(Bucket), Options(), db.Object);
        Assert.That(outcome, Is.EqualTo(BucketOutcome.VerifyFailed));
        Assert.That(events, Does.Not.Contain("delete"));
        Assert.That(events, Has.None.StartsWith("marker:"));
        Assert.That(markers, Is.Empty);
    }

    [Test]
    public async Task ProcessBucket_NeverDeletes_Mismatches()
    {
        service.Hot[Bucket] = Rows(Bucket);
        readBack = ReadBack.Mismatch;
        var outcome = await service.Process(Bucket, EligibleNow(Bucket), Options(), db.Object);
        Assert.That(outcome, Is.EqualTo(BucketOutcome.VerifyFailed));
        Assert.That(events, Does.Not.Contain("delete"));
        Assert.That(markers, Is.Empty);
    }

    [Test]
    public void ProcessBucket_NeverDeletes_WhenWriteThrows()
    {
        service.Hot[Bucket] = Rows(Bucket);
        writeThrows = true;
        Assert.ThrowsAsync<InvalidOperationException>(() => service.Process(Bucket, EligibleNow(Bucket), Options(), db.Object));
        Assert.That(events, Does.Not.Contain("delete"));
        Assert.That(markers, Is.Empty);
    }

    [Test]
    public async Task ProcessBucket_NeverDeletes_WhenDeleteFlagOff()
    {
        service.Hot[Bucket] = Rows(Bucket);
        var outcome = await service.Process(Bucket, EligibleNow(Bucket), Options(delete: false), db.Object);
        Assert.That(outcome, Is.EqualTo(BucketOutcome.Archived));
        Assert.That(events, Does.Not.Contain("delete"));
        Assert.That(markers["bazaar_archive:v2:" + Product + ":" + Bucket], Does.StartWith("archived:4:"));
    }

    [Test]
    public async Task ProcessBucket_NeverDeletes_WhenNotEligible()
    {
        service.Hot[Bucket] = Rows(Bucket);
        var outcome = await service.Process(Bucket, TooEarlyNow(Bucket), Options(), db.Object);
        Assert.That(outcome, Is.EqualTo(BucketOutcome.Archived));
        Assert.That(events, Does.Not.Contain("delete"));
    }

    [Test]
    public async Task ProcessBucket_DeleteFailure_KeepsArchivedMarker()
    {
        service.Hot[Bucket] = Rows(Bucket);
        service.DeleteThrows = new Exception("scylla down");
        var outcome = await service.Process(Bucket, EligibleNow(Bucket), Options(), db.Object);
        Assert.That(outcome, Is.EqualTo(BucketOutcome.Failed));
        Assert.That(markers["bazaar_archive:v2:" + Product + ":" + Bucket], Does.StartWith("archived:"));
        // retry succeeds and does not rewrite the blob
        service.DeleteThrows = null;
        events.Clear();
        Assert.That(await service.Process(Bucket, EligibleNow(Bucket), Options(), db.Object), Is.EqualTo(BucketOutcome.Deleted));
        Assert.That(events, Does.Not.Contain("write"));
    }

    [Test]
    public async Task ProcessBucket_SkipsWrite_WhenExistingV2BlobCoversRows()
    {
        var rows = Rows(Bucket);
        service.Hot[Bucket] = rows;
        blobs[Bucket] = ArchivedMinuteBlock.FromRows(Product, Bucket, rows);
        var outcome = await service.Process(Bucket, TooEarlyNow(Bucket), Options(), db.Object);
        Assert.That(outcome, Is.EqualTo(BucketOutcome.AlreadyArchived));
        Assert.That(events, Does.Not.Contain("write"));
        Assert.That(markers.Values.Single(), Does.StartWith("archived:4:"));
    }

    [Test]
    public async Task ProcessBucket_RewritesV1Blob()
    {
        var rows = Rows(Bucket);
        service.Hot[Bucket] = rows;
        var v1 = ArchivedMinuteBlock.FromRows(Product, Bucket, rows);
        v1.FormatVersion = 1;
        v1.SerialisedBuyOrders = null;
        v1.SerialisedSellOrders = null;
        v1.ReferenceId = null;
        v1.TimestampDeltasMs = null;
        blobs[Bucket] = v1;
        var outcome = await service.Process(Bucket, TooEarlyNow(Bucket), Options(), db.Object);
        Assert.That(outcome, Is.EqualTo(BucketOutcome.Archived));
        Assert.That(events, Does.Contain("write"));
        Assert.That(blobs[Bucket].FormatVersion, Is.EqualTo(2));
        Assert.That(blobs[Bucket].SerialisedBuyOrders, Is.Not.Null);
    }

    [Test]
    public async Task ProcessBucket_IgnoresV1DoneMarker()
    {
        service.Hot[Bucket] = Rows(Bucket);
        markers["bazaar_archive:" + Product + ":" + Bucket] = "done";
        var outcome = await service.Process(Bucket, TooEarlyNow(Bucket), Options(), db.Object);
        Assert.That(outcome, Is.EqualTo(BucketOutcome.Archived));
        Assert.That(events, Does.Contain("write"));
    }

    [Test]
    public async Task ProcessBucket_SkipsDeletedMarker()
    {
        service.Hot[Bucket] = Rows(Bucket);
        markers["bazaar_archive:v2:" + Product + ":" + Bucket] = "deleted:4:2026-01-01T00:00:00.0000000Z";
        var outcome = await service.Process(Bucket, EligibleNow(Bucket), Options(), db.Object);
        Assert.That(outcome, Is.EqualTo(BucketOutcome.Skipped));
        Assert.That(events, Is.Empty);
    }

    [Test]
    public async Task ProcessBucket_SkipsArchivedMarker_UntilEligible()
    {
        service.Hot[Bucket] = Rows(Bucket);
        markers["bazaar_archive:v2:" + Product + ":" + Bucket] = "archived:4:2026-01-01T00:00:00.0000000Z";
        Assert.That(await service.Process(Bucket, TooEarlyNow(Bucket), Options(), db.Object), Is.EqualTo(BucketOutcome.Skipped));
        Assert.That(events, Is.Empty);
        Assert.That(await service.Process(Bucket, EligibleNow(Bucket), Options(), db.Object), Is.EqualTo(BucketOutcome.Deleted));
    }

    [Test]
    public async Task ProcessBucket_LegacyOnlyBucket_MarksDeletedWithoutDelete()
    {
        service.Legacy[Bucket] = Rows(Bucket);
        var outcome = await service.Process(Bucket, EligibleNow(Bucket), Options(), db.Object);
        Assert.That(outcome, Is.EqualTo(BucketOutcome.Deleted));
        Assert.That(events, Does.Not.Contain("delete"));
        Assert.That(markers.Values.Single(), Does.StartWith("deleted:0:"));
        Assert.That(blobs, Contains.Key(Bucket));
    }

    [Test]
    public async Task ProcessBucket_DoesNotReadLegacy_WhenLegacyDropped()
    {
        service.Hot[Bucket] = Rows(Bucket);
        await service.Process(Bucket, TooEarlyNow(Bucket), Options(legacyDropped: true), db.Object);
        Assert.That(service.LegacyCalls, Is.EqualTo(0));
    }

    [Test]
    public async Task ProcessBucket_EmptyBucket_MarksEmpty()
    {
        var outcome = await service.Process(Bucket, TooEarlyNow(Bucket), Options(), db.Object);
        Assert.That(outcome, Is.EqualTo(BucketOutcome.Empty));
        Assert.That(markers.Values.Single(), Does.StartWith("empty:"));
        Assert.That(events, Does.Not.Contain("write"));
    }

    [Test]
    public async Task MaxDeletesPerSweep_StopsAfterLimit()
    {
        service.Hot[Bucket] = Rows(Bucket);
        service.Hot[Bucket - 1] = Rows(Bucket - 1);
        var o = Options(maxDeletes: 1);
        var now = EligibleNow(Bucket);
        Assert.That(await service.Process(Bucket, now, o, db.Object), Is.EqualTo(BucketOutcome.Deleted));
        Assert.That(await service.Process(Bucket - 1, now, o, db.Object), Is.EqualTo(BucketOutcome.Archived));
        Assert.That(events.Count(e => e == "delete"), Is.EqualTo(1));
        Assert.That(markers["bazaar_archive:v2:" + Product + ":" + (Bucket - 1)], Does.StartWith("archived:"));
    }

    [Test]
    public void ParseMarker_ReadsAllKinds()
    {
        Assert.That(HistoryArchiveService.ParseMarker("archived:12:2026-01-01T00:00:00.0000000Z"), Is.EqualTo(("archived", 12)));
        Assert.That(HistoryArchiveService.ParseMarker("deleted:0:2026-01-01T00:00:00.0000000Z"), Is.EqualTo(("deleted", 0)));
        Assert.That(HistoryArchiveService.ParseMarker("empty:2026-01-01T00:00:00.0000000Z").Kind, Is.EqualTo("empty"));
        Assert.That(HistoryArchiveService.ParseMarker("done").Kind, Is.Null);
        Assert.That(HistoryArchiveService.ParseMarker(null).Kind, Is.Null);
    }

    // ---- parallel product sweeps ----

    private class SweepService : HistoryArchiveService
    {
        public readonly string[] ProductList;
        public Func<string, Task> OnLoad = _ => Task.CompletedTask;
        public int Deletes;
        public readonly ConcurrentDictionary<string, string> Markers = new();
        public readonly ConcurrentDictionary<string, int> LoadsPerProduct = new();
        public readonly TaskCompletionSource Complete = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int NewestCold;

        public SweepService(string[] products, Dictionary<string, string> cfg)
            : base(new BazaarService(new ConfigurationBuilder().Build(), NullLogger<BazaarService>.Instance, Mock.Of<Cassandra.ISession>()),
                  MakeStore(), MakeRedis(out var markers), Cfg(cfg), new CapturingLogger())
        {
            ProductList = products;
            Markers = markers;
            ((CapturingLogger)Logger).Owner = this;
            NewestCold = NewestClosedBucket(DateTime.UtcNow, 7);
        }

        private ILogger<HistoryArchiveService> Logger => (ILogger<HistoryArchiveService>)typeof(HistoryArchiveService)
            .GetField("logger", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic).GetValue(this);

        private static IConfiguration Cfg(Dictionary<string, string> cfg)
        {
            var d = new Dictionary<string, string>
            {
                ["HISTORY_ARCHIVE:ENABLED"] = "true",
                ["HISTORY_ARCHIVE:START_DELAY_SECONDS"] = "0",
                ["HISTORY_ARCHIVE:PER_PRODUCT_PAUSE_MS"] = "0",
                ["HISTORY_ARCHIVE:PER_BUCKET_PAUSE_MS"] = "0",
                ["HISTORY_ARCHIVE:EMPTY_STREAK_LIMIT"] = "2",
                ["HISTORY_ARCHIVE:IDLE_BETWEEN_SWEEPS_SECONDS"] = "3600",
                ["HISTORY_ARCHIVE:RETENTION_HOT_DAYS"] = "0",
                ["HISTORY_ARCHIVE:DELETE_MARGIN_DAYS"] = "0",
            };
            foreach (var kv in cfg)
                d[kv.Key] = kv.Value;
            return new ConfigurationBuilder().AddInMemoryCollection(d).Build();
        }

        private static IBlobHistoryStore MakeStore()
        {
            var blobs = new ConcurrentDictionary<string, ArchivedMinuteBlock>();
            var m = new Mock<IBlobHistoryStore>();
            m.SetupGet(x => x.IsEnabled).Returns(true);
            m.Setup(x => x.WriteBlobAsync(It.IsAny<ArchivedMinuteBlock>(), It.IsAny<CancellationToken>()))
                .Returns<ArchivedMinuteBlock, CancellationToken>((b, _) => { blobs[b.ProductId + ":" + b.BucketId] = RoundTrip(b); return Task.CompletedTask; });
            m.Setup(x => x.ReadBlobUncachedAsync(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .Returns<string, int, CancellationToken>((p, b, _) => Task.FromResult(blobs.TryGetValue(p + ":" + b, out var blob) ? RoundTrip(blob) : null));
            return m.Object;
        }

        private static IConnectionMultiplexer MakeRedis(out ConcurrentDictionary<string, string> markers)
        {
            var mk = new ConcurrentDictionary<string, string>();
            markers = mk;
            var d = new Mock<IDatabase>();
            d.Setup(x => x.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
                .Returns<RedisKey, CommandFlags>((k, _) => Task.FromResult(mk.TryGetValue(k, out var v) ? (RedisValue)v : RedisValue.Null));
            d.Setup(x => x.KeyExistsAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
                .Returns<RedisKey, CommandFlags>((k, _) => Task.FromResult(mk.ContainsKey(k)));
            d.Setup(x => x.StringSetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<Expiration>(), It.IsAny<ValueCondition>(), It.IsAny<CommandFlags>()))
                .Returns<RedisKey, RedisValue, Expiration, ValueCondition, CommandFlags>((k, v, _, _, _) => { mk[k] = v; return Task.FromResult(true); });
            var r = new Mock<IConnectionMultiplexer>();
            r.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(d.Object);
            return r.Object;
        }

        public ConcurrentBag<string> Messages => ((CapturingLogger)Logger).Messages;

        protected override IReadOnlyList<string> GetProducts() => ProductList;

        protected override async Task<List<AggregatedQuickStatus>> LoadHotRowsAsync(string productId, int bucketId, DateTime start, DateTime end)
        {
            LoadsPerProduct.AddOrUpdate(productId, 1, (_, n) => n + 1);
            await OnLoad(productId).ConfigureAwait(false);
            // data only in the 3 buckets below the newest cold bucket: old enough to be delete eligible
            if (bucketId < NewestCold - 3 && bucketId >= NewestCold - 6)
                return Enumerable.Range(0, 3).Select(i => new AggregatedQuickStatus
                {
                    ProductId = productId, TimeStamp = start.AddDays(2).AddMinutes(5 * i), Count = 5, BuyPrice = 1, SellPrice = 1, ReferenceId = i + 1,
                    BuyOrders = new[] { new BuyOrder { Amount = 1, PricePerUnit = 1 } },
                    SellOrders = new[] { new SellOrder { Amount = 1, PricePerUnit = 1 } }
                }).ToList();
            return new List<AggregatedQuickStatus>();
        }

        protected override Task<List<AggregatedQuickStatus>> LoadLegacyRowsAsync(string productId, DateTime bucketStart, DateTime bucketEnd)
            => Task.FromResult(new List<AggregatedQuickStatus>());

        protected override Task DeleteHotPartitionsAsync(string productId, int bucketId, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Deletes);
            return Task.CompletedTask;
        }

        protected override Task RetryBackoffAsync(TimeSpan delay, CancellationToken cancellationToken) => Task.CompletedTask;

        public async Task RunSweepAsync()
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await StartAsync(cts.Token);
            await Task.WhenAny(Complete.Task, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.That(Complete.Task.IsCompleted, Is.True, "sweep did not complete");
            await StopAsync(CancellationToken.None);
        }
    }

    private class CapturingLogger : ILogger<HistoryArchiveService>
    {
        public SweepService Owner;
        public readonly ConcurrentBag<string> Messages = new();
        public IDisposable BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter)
        {
            var msg = formatter(state, exception);
            Messages.Add(msg);
            if (msg.Contains("Archive sweep complete"))
                Owner?.Complete.TrySetResult();
        }
    }

    private static string[] Names(int n) => Enumerable.Range(0, n).Select(i => "P" + i).ToArray();

    [Test]
    public async Task ParallelProducts_AreBoundedAndActuallyConcurrent()
    {
        var svc = new SweepService(Names(6), new() { ["HISTORY_ARCHIVE:PARALLEL_PRODUCTS"] = "3" });
        var inFlight = 0;
        var max = 0;
        var seen = new ConcurrentDictionary<string, bool>();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        svc.OnLoad = async p =>
        {
            if (!seen.TryAdd(p, true))
                return; // only gate the first load of each product
            var now = Interlocked.Increment(ref inFlight);
            int prev;
            do { prev = Volatile.Read(ref max); } while (now > prev && Interlocked.CompareExchange(ref max, now, prev) != prev);
            if (now >= 3)
                gate.TrySetResult();
            await Task.WhenAny(gate.Task, Task.Delay(200));
            await Task.Delay(30);
            Interlocked.Decrement(ref inFlight);
        };
        await svc.RunSweepAsync();
        Assert.That(max, Is.LessThanOrEqualTo(3));
        Assert.That(max, Is.GreaterThanOrEqualTo(2));
        Assert.That(seen.Count, Is.EqualTo(6));
        Assert.That(svc.Messages.Any(m => m.Contains("parallelProducts=3")), Is.True);
    }

    [Test]
    public async Task ParallelProducts_DefaultIsSequential()
    {
        var svc = new SweepService(Names(4), new());
        var inFlight = 0;
        var max = 0;
        var seen = new ConcurrentDictionary<string, bool>();
        svc.OnLoad = async p =>
        {
            if (!seen.TryAdd(p, true))
                return;
            var now = Interlocked.Increment(ref inFlight);
            int prev;
            do { prev = Volatile.Read(ref max); } while (now > prev && Interlocked.CompareExchange(ref max, now, prev) != prev);
            await Task.Delay(40);
            Interlocked.Decrement(ref inFlight);
        };
        await svc.RunSweepAsync();
        Assert.That(max, Is.EqualTo(1));
        Assert.That(seen.Count, Is.EqualTo(4));
    }

    [Test]
    public async Task ParallelProducts_MaxDeletesPerSweepIsNeverExceeded()
    {
        var svc = new SweepService(Names(3), new()
        {
            ["HISTORY_ARCHIVE:PARALLEL_PRODUCTS"] = "3",
            ["HISTORY_ARCHIVE:DELETE_ARCHIVED"] = "true",
            ["HISTORY_ARCHIVE:MAX_DELETES_PER_SWEEP"] = "5",
            ["HISTORY_ARCHIVE:EMPTY_STREAK_LIMIT"] = "50",
            ["HISTORY_ARCHIVE:OLDEST_BUCKET_ID"] = "0",
        });
        svc.OnLoad = _ => Task.Delay(1);
        await svc.RunSweepAsync();
        Assert.That(svc.Deletes, Is.EqualTo(5));
    }

    [Test]
    public async Task ParallelProducts_FailingProductDoesNotStopOthers()
    {
        var svc = new SweepService(Names(5), new() { ["HISTORY_ARCHIVE:PARALLEL_PRODUCTS"] = "2" });
        svc.OnLoad = p => p == "P1" ? throw new InvalidOperationException("boom") : Task.CompletedTask;
        await svc.RunSweepAsync();
        foreach (var p in Names(5))
            Assert.That(svc.LoadsPerProduct.ContainsKey(p), Is.True, p);
        Assert.That(svc.Messages.Any(m => m.Contains("problems=0")), Is.False);
        Assert.That(svc.Messages.Any(m => m.Contains("Archive sweep complete (problems=")), Is.True);
        Assert.That(svc.Markers.ContainsKey("bazaar_archive:v2:sweep_complete"), Is.False);
    }
}
