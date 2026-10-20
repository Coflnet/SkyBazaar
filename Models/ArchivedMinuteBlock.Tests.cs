using System;
using System.Linq;
using MessagePack;
using NUnit.Framework;

namespace Coflnet.Sky.SkyBazaar.Models;

public class ArchivedMinuteBlockTests
{
    /// <summary>Shape of the v1 blob (keys 0-17) as it was written before order books were archived.</summary>
    [MessagePackObject]
    public class V1Block
    {
        [Key(0)] public int FormatVersion { get; set; } = 1;
        [Key(1)] public string ProductId { get; set; }
        [Key(2)] public int BucketId { get; set; }
        [Key(3)] public long FirstTimestampTicks { get; set; }
        [Key(4)] public int[] TimestampDeltasSeconds { get; set; }
        [Key(5)] public double[] BuyPrice { get; set; }
        [Key(6)] public double[] SellPrice { get; set; }
        [Key(7)] public long[] BuyVolume { get; set; }
        [Key(8)] public long[] SellVolume { get; set; }
        [Key(9)] public long[] BuyMovingWeek { get; set; }
        [Key(10)] public long[] SellMovingWeek { get; set; }
        [Key(11)] public int[] BuyOrdersCount { get; set; }
        [Key(12)] public int[] SellOrdersCount { get; set; }
        [Key(13)] public float[] MaxBuy { get; set; }
        [Key(14)] public float[] MaxSell { get; set; }
        [Key(15)] public float[] MinBuy { get; set; }
        [Key(16)] public float[] MinSell { get; set; }
        [Key(17)] public short[] Count { get; set; }
    }

    private static readonly DateTime T0 = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);

    private static AggregatedQuickStatus Row(int i, bool buyBooks = true, bool sellBooks = true)
    {
        var row = new AggregatedQuickStatus
        {
            ProductId = "P",
            TimeStamp = T0.AddMinutes(5 * i).AddMilliseconds(250 + i),
            BuyPrice = 10 + i,
            SellPrice = 9 + i,
            BuyVolume = 100 + i,
            Count = (short)(3 + i),
            ReferenceId = 5_000_000_000L + i,
        };
        if (buyBooks)
            row.BuyOrders = new[] { new BuyOrder { Amount = 4, PricePerUnit = 12.5 + i } };
        if (sellBooks)
            row.SellOrders = new[] { new SellOrder { Amount = 1, PricePerUnit = 8.5 + i } };
        return row;
    }

    [Test]
    public void ArchivedMinuteBlock_RoundTripsOrderBooksReferenceIdAndMsTimestamps()
    {
        var rows = new[] { Row(0), Row(1, sellBooks: false), Row(2) };
        var block = ArchivedMinuteBlock.FromRows("P", 77, rows);
        var back = MessagePackSerializer.Deserialize<ArchivedMinuteBlock>(MessagePackSerializer.Serialize(block));
        var restored = back.ToRows().ToList();

        Assert.That(back.FormatVersion, Is.EqualTo(2));
        Assert.That(restored, Has.Count.EqualTo(3));
        for (var i = 0; i < 3; i++)
        {
            Assert.That(restored[i].TimeStamp.Ticks / TimeSpan.TicksPerMillisecond, Is.EqualTo(rows[i].TimeStamp.Ticks / TimeSpan.TicksPerMillisecond));
            Assert.That(restored[i].ReferenceId, Is.EqualTo(rows[i].ReferenceId));
            Assert.That(restored[i].BuyPrice, Is.EqualTo(rows[i].BuyPrice));
            Assert.That(restored[i].SerialisedBuyOrders, Is.EqualTo(rows[i].SerialisedBuyOrders));
        }
        Assert.That(restored[1].SerialisedSellOrders, Is.Null);
        Assert.That(restored[1].SellOrders, Is.Null);
        Assert.That(restored[0].BuyOrders.First().PricePerUnit, Is.EqualTo(12.5));
    }

    [Test]
    public void ArchivedMinuteBlock_V1PayloadDeserialisesWithoutOrderBooks()
    {
        var v1 = new V1Block
        {
            ProductId = "P",
            BucketId = 77,
            FirstTimestampTicks = T0.Ticks,
            TimestampDeltasSeconds = new[] { 0, 300 },
            BuyPrice = new[] { 1d, 2d },
            SellPrice = new[] { 1d, 2d },
            BuyVolume = new[] { 1L, 2L },
            SellVolume = new[] { 1L, 2L },
            BuyMovingWeek = new[] { 1L, 2L },
            SellMovingWeek = new[] { 1L, 2L },
            BuyOrdersCount = new[] { 1, 2 },
            SellOrdersCount = new[] { 1, 2 },
            MaxBuy = new[] { 1f, 2f },
            MaxSell = new[] { 1f, 2f },
            MinBuy = new[] { 1f, 2f },
            MinSell = new[] { 1f, 2f },
            Count = new short[] { 1, 2 },
        };
        var block = MessagePackSerializer.Deserialize<ArchivedMinuteBlock>(MessagePackSerializer.Serialize(v1));
        var rows = block.ToRows().ToList();
        Assert.That(block.FormatVersion, Is.EqualTo(1));
        Assert.That(block.SerialisedBuyOrders, Is.Null);
        Assert.That(rows, Has.Count.EqualTo(2));
        Assert.That(rows[1].TimeStamp, Is.EqualTo(T0.AddSeconds(300)));
        Assert.That(rows[1].SerialisedBuyOrders, Is.Null);
        Assert.That(rows[1].ReferenceId, Is.EqualTo(0));
        Assert.That(rows[1].BuyPrice, Is.EqualTo(2));
    }

    private static ArchivedMinuteBlock AsV1(ArchivedMinuteBlock b)
    {
        b.FormatVersion = 1;
        b.SerialisedBuyOrders = null;
        b.SerialisedSellOrders = null;
        b.ReferenceId = null;
        b.TimestampDeltasMs = null;
        return b;
    }

    [Test]
    public void CoversRows_FalseForV1Blob_WhenRowsHaveOrderBooks()
    {
        var rows = new[] { Row(0), Row(1) };
        var v1 = AsV1(ArchivedMinuteBlock.FromRows("P", 77, rows));
        Assert.That(v1.CoversRows(rows), Is.False);
        Assert.That(ArchivedMinuteBlock.FromRows("P", 77, rows).CoversRows(rows), Is.True);
    }

    [Test]
    public void CoversRows_TrueWhenRowsHaveNone()
    {
        var rows = new[] { Row(0, false, false), Row(1, false, false) };
        // v1 blobs only have second precision, so use whole-second timestamps for the comparison
        foreach (var r in rows)
            r.TimeStamp = new DateTime(r.TimeStamp.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, DateTimeKind.Utc);
        var v1 = AsV1(ArchivedMinuteBlock.FromRows("P", 77, rows));
        Assert.That(v1.CoversRows(rows), Is.True);
    }

    [Test]
    public void CoversRows_FalseWhenTimestampMissing()
    {
        var block = ArchivedMinuteBlock.FromRows("P", 77, new[] { Row(0) });
        Assert.That(block.CoversRows(new[] { Row(0), Row(1) }), Is.False);
    }

    [Test]
    public void SameRows_DetectsMissingOrderBookOnReadBack()
    {
        var rows = new[] { Row(0), Row(1) };
        var written = ArchivedMinuteBlock.FromRows("P", 77, rows);
        var readBack = MessagePackSerializer.Deserialize<ArchivedMinuteBlock>(MessagePackSerializer.Serialize(written));
        Assert.That(written.SameRows(readBack), Is.True);
        readBack.SerialisedBuyOrders[1] = null;
        Assert.That(written.SameRows(readBack), Is.False);
        readBack = MessagePackSerializer.Deserialize<ArchivedMinuteBlock>(MessagePackSerializer.Serialize(written));
        readBack.ReferenceId[0]++;
        Assert.That(written.SameRows(readBack), Is.False);
        Assert.That(written.SameRows(null), Is.False);
    }

    [Test]
    public void EstimatedBytes_IncludesOrderBookPayload()
    {
        var withBooks = ArchivedMinuteBlock.FromRows("P", 77, Enumerable.Range(0, 100).Select(i => Row(i)).ToList());
        var without = ArchivedMinuteBlock.FromRows("P", 77, Enumerable.Range(0, 100).Select(i => Row(i, false, false)).ToList());
        var payload = withBooks.SerialisedBuyOrders.Concat(withBooks.SerialisedSellOrders).Sum(b => (long)b.Length);
        Assert.That(withBooks.EstimatedBytes() - without.EstimatedBytes(), Is.GreaterThanOrEqualTo(payload));
        Assert.That(without.EstimatedBytes(), Is.GreaterThan(100 * 80));
    }
}
