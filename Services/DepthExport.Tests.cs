using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.SkyAuctionTracker.Controllers;
using Coflnet.Sky.SkyBazaar.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using NUnit.Framework;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class DepthExportTests
{
    private static readonly DateTime T0 = new(2026, 9, 1, 12, 0, 0, DateTimeKind.Utc);

    private static AggregatedQuickStatus Row(DateTime t, int depth = 5)
    {
        // buyOrders = ASK side ascending, sellOrders = BID side descending (buyOrders[0].pricePerUnit == buyPrice)
        var asks = Enumerable.Range(0, depth).Select(i => new Order { Amount = 10 + i, PricePerUnit = 100 + i, Orders = (short)(1 + i) }).ToList();
        var bids = Enumerable.Range(0, depth).Select(i => new Order { Amount = 20 + i, PricePerUnit = 90 - i, Orders = (short)(2 + i) }).ToList();
        return new AggregatedQuickStatus
        {
            ProductId = "P", TimeStamp = t, BuyPrice = 100, SellPrice = 90, BuyVolume = 1, SellVolume = 2, BuyMovingWeek = 3, SellMovingWeek = 4,
            BuyOrdersCount = 5, SellOrdersCount = 6, MinBuy = 99, MaxBuy = 101, MinSell = 89, MaxSell = 91, Count = 2,
            BuyOrders = asks, SellOrders = bids,
        };
    }

    private static async IAsyncEnumerable<DepthSnapshot> Stream(IEnumerable<DepthSnapshot> rows, [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var r in rows)
        {
            ct.ThrowIfCancellationRequested();
            yield return r;
            await Task.Yield();
        }
    }

    private static async Task<JsonDocument> Export(IEnumerable<DepthSnapshot> rows, int levels)
    {
        var ms = new MemoryStream();
        await DepthExportWriter.WriteAsync(ms, Stream(rows), levels, CancellationToken.None);
        return JsonDocument.Parse(ms.ToArray());
    }

    [Test]
    public async Task ProjectionHasSchemaFieldOrderAndTruncatesLevels()
    {
        using var doc = await Export(new[] { DepthSnapshot.From(Row(T0, depth: 8)) }, levels: 3);
        Assert.That(doc.RootElement.GetProperty("schema").GetString(), Is.EqualTo("depth-compact-v1"));
        Assert.That(doc.RootElement.GetProperty("fields").EnumerateArray().Select(f => f.GetString()), Is.EqualTo(new[]
        {
            "ts", "bid", "ask", "buyVolume", "sellVolume", "buyMovingWeek", "sellMovingWeek", "buyOrdersCount", "sellOrdersCount",
            "minAsk", "maxAsk", "minBid", "maxBid", "bids", "asks"
        }));
        var row = doc.RootElement.GetProperty("rows")[0];
        Assert.That(row.GetArrayLength(), Is.EqualTo(15));
        Assert.That(row[0].GetInt64(), Is.EqualTo(new DateTimeOffset(T0).ToUnixTimeSeconds()));
        Assert.That(row.EnumerateArray().Skip(3).Take(10).Select(e => e.GetDouble()), Is.EqualTo(new double[] { 1, 2, 3, 4, 5, 6, 99, 101, 89, 91 }));
        Assert.That(row[13].GetArrayLength(), Is.EqualTo(3));
        Assert.That(row[14].GetArrayLength(), Is.EqualTo(3));
    }

    [Test]
    public async Task BidsAreTheSellOrdersDescendingAndAsksTheBuyOrdersAscending()
    {
        using var doc = await Export(new[] { DepthSnapshot.From(Row(T0)) }, levels: 4);
        var row = doc.RootElement.GetProperty("rows")[0];
        var bids = row[13].EnumerateArray().Select(l => l[0].GetDouble()).ToList();
        var asks = row[14].EnumerateArray().Select(l => l[0].GetDouble()).ToList();
        Assert.That(bids, Is.EqualTo(new[] { 90d, 89, 88, 87 }));
        Assert.That(asks, Is.EqualTo(new[] { 100d, 101, 102, 103 }));
        Assert.That(row[1].GetDouble(), Is.EqualTo(bids[0]), "bid column is the best bid");
        Assert.That(row[2].GetDouble(), Is.EqualTo(asks[0]), "ask column is the best ask");
        Assert.That(row[13][0].EnumerateArray().Select(e => e.GetDouble()), Is.EqualTo(new[] { 90d, 20, 2 }), "[price, amount, orders]");
        Assert.That(row[14][1].EnumerateArray().Select(e => e.GetDouble()), Is.EqualTo(new[] { 101d, 11, 2 }));
    }

    [Test]
    public async Task ShallowOrMissingBooksYieldFewerLevelsAndCorruptBlobsStayValidJson()
    {
        var shallow = DepthSnapshot.From(Row(T0, depth: 2));
        var noBooks = DepthSnapshot.From(new AggregatedQuickStatus { ProductId = "P", TimeStamp = T0.AddMinutes(5), BuyPrice = 1, SellPrice = 1 });
        var corrupt = DepthSnapshot.From(new AggregatedQuickStatus { ProductId = "P", TimeStamp = T0.AddMinutes(10), SerialisedBuyOrders = new byte[] { 0x92, 0x93, 0x01 }, SerialisedSellOrders = new byte[] { 0xc1 } });
        using var doc = await Export(new[] { shallow, noBooks, corrupt }, levels: 30);
        var rows = doc.RootElement.GetProperty("rows");
        Assert.That(rows.GetArrayLength(), Is.EqualTo(3));
        Assert.That(rows[0][13].GetArrayLength(), Is.EqualTo(2));
        Assert.That(rows[1][13].GetArrayLength(), Is.EqualTo(0));
        Assert.That(rows[2][14].GetArrayLength(), Is.EqualTo(0));
    }

    [Test]
    public async Task CadenceIsNeverCoarsenedEveryStoredRowIsExported()
    {
        var times = Enumerable.Range(0, 1500).Select(i => T0.AddSeconds(i * 10)).ToList(); // finer than the 5 minute cold cadence
        using var doc = await Export(times.Select(t => DepthSnapshot.From(Row(t, 2))), levels: 1);
        var ts = doc.RootElement.GetProperty("rows").EnumerateArray().Select(r => r[0].GetInt64()).ToList();
        Assert.That(ts, Is.EqualTo(times.Select(t => new DateTimeOffset(t).ToUnixTimeSeconds())));
    }

    [Test]
    public void ColdBlockIsProjectedFromColumnsWithinTheWindowOnly()
    {
        var rows = Enumerable.Range(0, 10).Select(i => Row(T0.AddMinutes(5 * i))).ToList();
        var block = ArchivedMinuteBlock.FromRows("P", 1, rows);
        var projected = DepthSnapshot.FromBlock(block, T0.AddMinutes(5), T0.AddMinutes(25)).ToList(); // (start, end]
        Assert.That(projected.Select(p => p.UnixSeconds), Is.EqualTo(rows.Skip(2).Take(4).Select(r => new DateTimeOffset(r.TimeStamp).ToUnixTimeSeconds())));
        Assert.That(projected[0].Ask, Is.EqualTo(100));
        Assert.That(projected[0].Bid, Is.EqualTo(90));
        Assert.That(projected[0].MinAsk, Is.EqualTo(99f));
        Assert.That(projected[0].Asks, Is.EqualTo(rows[2].SerialisedBuyOrders));
    }

    [Test]
    public void RawSnapshotsWithoutAggregatesUseTheirOwnPriceForMinMax()
    {
        var raw = DepthSnapshot.From(new StorageQuickStatus { TimeStamp = T0, BuyPrice = 12, SellPrice = 10 });
        Assert.That((raw.MinAsk, raw.MaxAsk, raw.MinBid, raw.MaxBid), Is.EqualTo((12f, 12f, 10f, 10f)));
    }

    // ---- controller contract ----

    private class FakeDepth : DepthExportService
    {
        public Func<CancellationToken, IAsyncEnumerable<DepthSnapshot>> Source = ct => Stream(new[] { DepthSnapshot.From(Row(T0)) }, ct);
        public FakeDepth(int max = 4) : base(null, new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string> { ["DEPTH_EXPORT:MAX_CONCURRENT"] = max.ToString() }).Build()) { }
        public override IAsyncEnumerable<DepthSnapshot> Read(string itemId, DateTime start, DateTime end, CancellationToken ct) => Source(ct);
    }

    private static (BazaarController Controller, MemoryStream Body, DefaultHttpContext Http) Create(DepthExportService depth, CancellationToken aborted = default)
    {
        var http = new DefaultHttpContext();
        var body = new MemoryStream();
        http.Response.Body = body;
        http.RequestAborted = aborted;
        return (new BazaarController(null, null, depth) { ControllerContext = new ControllerContext { HttpContext = http } }, body, http);
    }

    [TestCase(null, "2026-09-02T00:00:00Z", 10)]
    [TestCase("2026-09-01T00:00:00Z", null, 10)]
    [TestCase("2026-09-02T00:00:00Z", "2026-09-01T00:00:00Z", 10)]
    [TestCase("2026-08-01T00:00:00Z", "2026-09-02T00:00:00Z", 10)] // 32 days
    [TestCase("2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z", 0)]
    [TestCase("2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z", 31)]
    public async Task InvalidWindowOrLevelsReturn400(string start, string end, int levels)
    {
        var (controller, _, _) = Create(new FakeDepth());
        var result = await controller.ExportDepth("P", start == null ? null : DateTime.Parse(start).ToUniversalTime(), end == null ? null : DateTime.Parse(end).ToUniversalTime(), levels);
        Assert.That(result, Is.TypeOf<BadRequestObjectResult>());
    }

    [Test]
    public async Task ThirtyOneDaysAndThirtyLevelsAreAccepted()
    {
        var (controller, body, _) = Create(new FakeDepth());
        var result = await controller.ExportDepth("P", T0, T0.AddDays(31), 30);
        Assert.That(result, Is.TypeOf<EmptyResult>());
        using var doc = JsonDocument.Parse(body.ToArray());
        Assert.That(doc.RootElement.GetProperty("rows").GetArrayLength(), Is.EqualTo(1));
    }

    [Test]
    public async Task SaturatedExportReturns429WithRetryAfterAndFreesTheSlotAfterwards()
    {
        var release = new TaskCompletionSource();
        var entered = new TaskCompletionSource();
        var depth = new FakeDepth(max: 1);
        depth.Source = ct => Blocked(release.Task, entered, ct);
        var (first, _, _) = Create(depth);
        var running = first.ExportDepth("P", T0, T0.AddDays(1), 5);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        var (second, _, _) = Create(depth);
        var rejected = await second.ExportDepth("P", T0, T0.AddDays(1), 5);
        Assert.That(((ObjectResult)rejected).StatusCode, Is.EqualTo(429));
        Assert.That(second.Response.Headers.RetryAfter.ToString(), Is.Not.Empty);

        release.SetResult();
        Assert.That(await running, Is.TypeOf<EmptyResult>());
        depth.Source = ct => Stream(new[] { DepthSnapshot.From(Row(T0)) }, ct);
        var (third, _, _) = Create(depth);
        Assert.That(await third.ExportDepth("P", T0, T0.AddDays(1), 5), Is.TypeOf<EmptyResult>());
    }

    private static async IAsyncEnumerable<DepthSnapshot> Blocked(Task release, TaskCompletionSource entered, [EnumeratorCancellation] CancellationToken ct)
    {
        entered.TrySetResult();
        await release.WaitAsync(ct);
        yield return DepthSnapshot.From(Row(T0));
    }

    [Test]
    public async Task AbortedRequestCancelsTheReadAndReleasesTheSlot()
    {
        using var aborted = new CancellationTokenSource();
        var entered = new TaskCompletionSource();
        var depth = new FakeDepth(max: 1);
        depth.Source = ct => Blocked(new TaskCompletionSource().Task, entered, ct);
        var (controller, _, _) = Create(depth, aborted.Token);
        var running = controller.ExportDepth("P", T0, T0.AddDays(1), 5);
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
        aborted.Cancel();
        var result = await running.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.That(((ObjectResult)result).StatusCode, Is.EqualTo(499));
        Assert.That(depth.TryEnter(), Is.True, "slot must be free again");
        depth.Exit();
    }

    // ---- benchmark of the projection against the old per-row object graph ----

    private static async IAsyncEnumerable<DepthSnapshot> Failing()
    {
        await Task.Yield();
        throw new InvalidOperationException("db down");
#pragma warning disable CS0162
        yield break;
#pragma warning restore CS0162
    }

    [Test]
    public async Task FailureBeforeTheFirstRowIsACleanServerErrorWithNoBody()
    {
        var depth = new FakeDepth();
        depth.Source = _ => Failing();
        var (controller, body, _) = Create(depth);
        var result = await controller.ExportDepth("P", T0, T0.AddDays(1), 5);
        Assert.That(((ObjectResult)result).StatusCode, Is.EqualTo(500));
        Assert.That(body.Length, Is.Zero);
        Assert.That(depth.TryEnter(), Is.True);
    }

    [Test]
    public async Task EmptyResultIsStillAValidDocument()
    {
        var depth = new FakeDepth();
        depth.Source = ct => Stream(Array.Empty<DepthSnapshot>(), ct);
        var (controller, body, _) = Create(depth);
        Assert.That(await controller.ExportDepth("P", T0, T0.AddDays(1), 5), Is.TypeOf<EmptyResult>());
        using var doc = JsonDocument.Parse(body.ToArray());
        Assert.That(doc.RootElement.GetProperty("rows").GetArrayLength(), Is.Zero);
    }

    [Test]
    public async Task TierBoundaryOverlapsAreDroppedKeepingAscendingOrder()
    {
        var a = T0; var b = T0.AddMinutes(5); var c = T0.AddMinutes(10);
        var rows = new[] { a, b, b.AddMilliseconds(300), b, c }.Select(t => DepthSnapshot.From(Row(t, 1)));
        var result = new List<long>();
        await foreach (var r in BazaarService.DropNotAfterPrevious(Stream(rows))) result.Add(r.UnixSeconds);
        Assert.That(result, Is.EqualTo(new[] { a, b, c }.Select(t => new DateTimeOffset(t).ToUnixTimeSeconds())));
    }

    [Test, Category("Benchmark")]
    public async Task ProjectionIsFasterThanDeserialisingFullBooksToJson()
    {
        const int rowCount = 4000, depth = 150;
        var rows = Enumerable.Range(0, rowCount).Select(i => Row(T0.AddSeconds(i * 20), depth)).ToList();

        var sw = Stopwatch.StartNew();
        var oldBytes = 0L;
        foreach (var r in rows) // what GetData + JSON output did per row: materialise both full books and serialise them
            oldBytes += JsonSerializer.SerializeToUtf8Bytes(new { r.TimeStamp, r.BuyPrice, r.SellPrice, buy = r.BuyOrders.ToList(), sell = r.SellOrders.ToList() }).Length;
        var oldMs = sw.Elapsed.TotalMilliseconds;

        sw.Restart();
        var ms = new MemoryStream();
        await DepthExportWriter.WriteAsync(ms, Stream(rows.Select(DepthSnapshot.From)), 10, CancellationToken.None);
        var newMs = sw.Elapsed.TotalMilliseconds;

        TestContext.Out.WriteLine($"rows={rowCount} depth={depth}/side: old per-row path {oldMs:F0} ms ({oldBytes / 1024} KiB), compact projection {newMs:F0} ms ({ms.Length / 1024} KiB), speedup x{oldMs / Math.Max(newMs, 0.01):F1}");
        Assert.That(ms.Length, Is.LessThan(oldBytes / 5));
        Assert.That(newMs * 3, Is.LessThan(oldMs), "generous ratio; real speedup is ~30x");
    }

    [Test]
    public void ConfigurationControlsConcurrencyAndTimeout()
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string> { ["DEPTH_EXPORT:MAX_CONCURRENT"] = "2", ["DEPTH_EXPORT:TIMEOUT_SECONDS"] = "7" }).Build();
        var service = new DepthExportService(null, config);
        Assert.That((service.MaxConcurrent, service.Timeout), Is.EqualTo((2, TimeSpan.FromSeconds(7))));
        Assert.That(new DepthExportService(null, new ConfigurationBuilder().Build()).MaxConcurrent, Is.EqualTo(4));
    }
}
