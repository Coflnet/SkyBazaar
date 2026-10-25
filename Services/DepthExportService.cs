using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.SkyBazaar.Models;
using MessagePack;
using Microsoft.Extensions.Configuration;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

/// <summary>
/// One stored order-book snapshot reduced to what the compact depth export needs.
/// The order books stay serialised (MessagePack) until the writer needs the top levels.
/// </summary>
public sealed class DepthSnapshot
{
    /// <summary>Snapshot time as whole unix seconds.</summary>
    public long UnixSeconds { get; init; }
    /// <summary>Best bid (what a sell order / instant sell gets), stored as SellPrice.</summary>
    public double Bid { get; init; }
    /// <summary>Best ask (what instant buy pays), stored as BuyPrice.</summary>
    public double Ask { get; init; }
    public long BuyVolume { get; init; }
    public long SellVolume { get; init; }
    public long BuyMovingWeek { get; init; }
    public long SellMovingWeek { get; init; }
    public int BuyOrdersCount { get; init; }
    public int SellOrdersCount { get; init; }
    public float MinAsk { get; init; }
    public float MaxAsk { get; init; }
    public float MinBid { get; init; }
    public float MaxBid { get; init; }
    /// <summary>Serialised ask side (stored as buyOrders), ascending by price.</summary>
    public byte[] Asks { get; init; }
    /// <summary>Serialised bid side (stored as sellOrders), descending by price.</summary>
    public byte[] Bids { get; init; }

    private static readonly long UnixEpochTicks = DateTime.UnixEpoch.Ticks;

    internal static long ToUnixSeconds(DateTime t) => (t.Ticks - UnixEpochTicks) / TimeSpan.TicksPerSecond;

    /// <summary>Projects a database row. Rows without aggregate min/max (raw snapshots) use their own price.</summary>
    public static DepthSnapshot From(StorageQuickStatus s)
    {
        var agg = s as AggregatedQuickStatus;
        var raw = agg == null || agg.Count == 0 && agg.MaxBuy == 0 && agg.MinBuy == 0 && agg.MaxSell == 0 && agg.MinSell == 0;
        return new DepthSnapshot
        {
            UnixSeconds = ToUnixSeconds(s.TimeStamp),
            Bid = s.SellPrice,
            Ask = s.BuyPrice,
            BuyVolume = s.BuyVolume,
            SellVolume = s.SellVolume,
            BuyMovingWeek = s.BuyMovingWeek,
            SellMovingWeek = s.SellMovingWeek,
            BuyOrdersCount = s.BuyOrdersCount,
            SellOrdersCount = s.SellOrdersCount,
            MinAsk = raw ? (float)s.BuyPrice : agg.MinBuy,
            MaxAsk = raw ? (float)s.BuyPrice : agg.MaxBuy,
            MinBid = raw ? (float)s.SellPrice : agg.MinSell,
            MaxBid = raw ? (float)s.SellPrice : agg.MaxSell,
            Asks = s.SerialisedBuyOrders,
            Bids = s.SerialisedSellOrders,
        };
    }

    /// <summary>
    /// Projects the rows of a cold-tier block inside (start, end] straight from its columns,
    /// without materialising an <see cref="AggregatedQuickStatus"/> per row.
    /// </summary>
    public static IEnumerable<DepthSnapshot> FromBlock(ArchivedMinuteBlock b, DateTime start, DateTime end)
    {
        var useMs = b.TimestampDeltasMs != null;
        var n = useMs ? b.TimestampDeltasMs.Length : b.TimestampDeltasSeconds?.Length ?? 0;
        for (var i = 0; i < n; i++)
        {
            var ticks = b.FirstTimestampTicks + (useMs ? b.TimestampDeltasMs[i] * TimeSpan.TicksPerMillisecond : (long)b.TimestampDeltasSeconds[i] * TimeSpan.TicksPerSecond);
            if (ticks <= start.Ticks || ticks > end.Ticks)
                continue;
            yield return new DepthSnapshot
            {
                UnixSeconds = (ticks - UnixEpochTicks) / TimeSpan.TicksPerSecond,
                Bid = b.SellPrice[i],
                Ask = b.BuyPrice[i],
                BuyVolume = b.BuyVolume[i],
                SellVolume = b.SellVolume[i],
                BuyMovingWeek = b.BuyMovingWeek[i],
                SellMovingWeek = b.SellMovingWeek[i],
                BuyOrdersCount = b.BuyOrdersCount[i],
                SellOrdersCount = b.SellOrdersCount[i],
                MinAsk = b.MinBuy[i],
                MaxAsk = b.MaxBuy[i],
                MinBid = b.MinSell[i],
                MaxBid = b.MaxSell[i],
                Asks = b.SerialisedBuyOrders != null && i < b.SerialisedBuyOrders.Length ? b.SerialisedBuyOrders[i] : null,
                Bids = b.SerialisedSellOrders != null && i < b.SerialisedSellOrders.Length ? b.SerialisedSellOrders[i] : null,
            };
        }
    }
}

/// <summary>
/// Writes the "depth-compact-v1" document. Only the first N levels of each serialised book are
/// decoded (straight from the MessagePack bytes, no object graph) and JSON is written incrementally.
/// </summary>
public static class DepthExportWriter
{
    public const string Schema = "depth-compact-v1";
    public static readonly string[] Fields =
    {
        "ts", "bid", "ask", "buyVolume", "sellVolume", "buyMovingWeek", "sellMovingWeek",
        "buyOrdersCount", "sellOrdersCount", "minAsk", "maxAsk", "minBid", "maxBid", "bids", "asks"
    };

    private struct Level
    {
        public double Price;
        public int Amount;
        public short Orders;
    }

    private const int FlushEveryRows = 256;

    /// <summary>Streams the document to <paramref name="output"/>; rows are flushed in batches.</summary>
    public static async Task<int> WriteAsync(Stream output, IAsyncEnumerable<DepthSnapshot> snapshots, int levels, CancellationToken ct)
    {
        await using var writer = new Utf8JsonWriter(output, new JsonWriterOptions { SkipValidation = true });
        writer.WriteStartObject();
        writer.WriteString("schema", Schema);
        writer.WriteStartArray("fields");
        foreach (var f in Fields)
            writer.WriteStringValue(f);
        writer.WriteEndArray();
        writer.WriteStartArray("rows");
        var rows = 0;
        await foreach (var snapshot in snapshots.WithCancellation(ct).ConfigureAwait(false))
        {
            WriteRow(writer, snapshot, levels);
            if (++rows % FlushEveryRows == 0)
                await writer.FlushAsync(ct).ConfigureAwait(false);
        }
        writer.WriteEndArray();
        writer.WriteEndObject();
        await writer.FlushAsync(ct).ConfigureAwait(false);
        return rows;
    }

    internal static void WriteRow(Utf8JsonWriter w, DepthSnapshot s, int levels)
    {
        w.WriteStartArray();
        w.WriteNumberValue(s.UnixSeconds);
        Number(w, s.Bid);
        Number(w, s.Ask);
        w.WriteNumberValue(s.BuyVolume);
        w.WriteNumberValue(s.SellVolume);
        w.WriteNumberValue(s.BuyMovingWeek);
        w.WriteNumberValue(s.SellMovingWeek);
        w.WriteNumberValue(s.BuyOrdersCount);
        w.WriteNumberValue(s.SellOrdersCount);
        Number(w, s.MinAsk);
        Number(w, s.MaxAsk);
        Number(w, s.MinBid);
        Number(w, s.MaxBid);
        WriteLevels(w, s.Bids, levels);
        WriteLevels(w, s.Asks, levels);
        w.WriteEndArray();
    }

    private static void Number(Utf8JsonWriter w, float v)
    {
        if (float.IsFinite(v)) w.WriteNumberValue(v); else w.WriteNullValue();
    }

    private static void Number(Utf8JsonWriter w, double v)
    {
        if (double.IsFinite(v)) w.WriteNumberValue(v); else w.WriteNullValue();
    }

    private static void WriteLevels(Utf8JsonWriter w, byte[] blob, int levels)
    {
        // Decode fully into a stack buffer first so a corrupt blob can never leave a half written array.
        Span<Level> buffer = stackalloc Level[levels];
        var count = Parse(blob, buffer);
        w.WriteStartArray();
        for (var i = 0; i < count; i++)
        {
            w.WriteStartArray();
            Number(w, buffer[i].Price);
            w.WriteNumberValue(buffer[i].Amount);
            w.WriteNumberValue(buffer[i].Orders);
            w.WriteEndArray();
        }
        w.WriteEndArray();
    }

    private static int Parse(byte[] blob, Span<Level> buffer)
    {
        if (blob == null || blob.Length == 0)
            return 0;
        var n = 0;
        try
        {
            var reader = new MessagePackReader(blob);
            if (reader.TryReadNil())
                return 0;
            var count = reader.ReadArrayHeader();
            // Order is [amount, pricePerUnit, orders]; levels beyond the requested depth are never read.
            for (var i = 0; i < count && n < buffer.Length; i++)
            {
                if (reader.ReadArrayHeader() < 3)
                    break;
                var amount = reader.ReadInt32();
                var price = reader.ReadDouble();
                var orders = reader.ReadInt16();
                buffer[n++] = new Level { Price = price, Amount = amount, Orders = orders };
            }
        }
        catch (Exception e) when (e is MessagePackSerializationException or EndOfStreamException or InvalidOperationException or OverflowException)
        {
            // keep the levels decoded so far
        }
        return n;
    }
}

/// <summary>
/// Gate and data source for the bulk depth export: validation, a global concurrency limit
/// (DEPTH_EXPORT:MAX_CONCURRENT, default 4) and the request timeout (DEPTH_EXPORT:TIMEOUT_SECONDS, default 120).
/// </summary>
public class DepthExportService
{
    public const int MaxWindowDays = 31;
    public const int MaxLevels = 30;
    public const int DefaultLevels = 10;

    private readonly BazaarService bazaar;
    private readonly SemaphoreSlim gate;

    public TimeSpan Timeout { get; }
    public int MaxConcurrent { get; }

    public DepthExportService(BazaarService bazaar, IConfiguration config)
    {
        this.bazaar = bazaar;
        MaxConcurrent = int.TryParse(config["DEPTH_EXPORT:MAX_CONCURRENT"], out var max) && max > 0 ? max : 4;
        Timeout = TimeSpan.FromSeconds(int.TryParse(config["DEPTH_EXPORT:TIMEOUT_SECONDS"], out var t) && t > 0 ? t : 120);
        gate = new SemaphoreSlim(MaxConcurrent, MaxConcurrent);
    }

    /// <summary>Takes a slot without waiting; false when the export is saturated.</summary>
    public bool TryEnter() => gate.Wait(0);

    /// <summary>Returns a slot taken by <see cref="TryEnter"/>.</summary>
    public void Exit() => gate.Release();

    /// <summary>Returns an error message, or null when the request is acceptable.</summary>
    public static string Validate(DateTime? start, DateTime? end, int levels)
    {
        if (start == null || end == null)
            return "start and end are required";
        if (end <= start)
            return "end must be after start";
        if (end.Value - start.Value > TimeSpan.FromDays(MaxWindowDays))
            return $"window must not exceed {MaxWindowDays} days";
        if (levels < 1 || levels > MaxLevels)
            return $"levels must be between 1 and {MaxLevels}";
        return null;
    }

    /// <summary>Snapshots in ascending time order at the finest stored cadence.</summary>
    public virtual IAsyncEnumerable<DepthSnapshot> Read(string itemId, DateTime start, DateTime end, CancellationToken ct)
        => bazaar.StreamDepthSnapshots(itemId, start, end, ct);
}
