using System;
using System.Collections.Generic;
using System.Linq;
using MessagePack;

namespace Coflnet.Sky.SkyBazaar.Models
{
    /// <summary>
    /// Column-oriented, MessagePack-serializable representation of one cold-tier
    /// 30-day archive block of 5-minute aggregated data for a single product.
    /// Brotli is applied on top of MessagePack to compress aggressively.
    /// </summary>
    [MessagePackObject]
    public class ArchivedMinuteBlock
    {
        /// <summary>Blob format version (1 = no order books, 2 = order books, reference ids and ms timestamps); bump on any breaking schema change.</summary>
        [Key(0)] public int FormatVersion { get; set; } = CurrentFormatVersion;
        /// <summary>Product (item) the block belongs to.</summary>
        [Key(1)] public string ProductId { get; set; }
        /// <summary>30-day bucket id, anchored to <see cref="ArchivedMinuteBlock.Epoch"/>.</summary>
        [Key(2)] public int BucketId { get; set; }
        /// <summary>UTC ticks of the earliest row in this block; deltas are relative to this.</summary>
        [Key(3)] public long FirstTimestampTicks { get; set; }
        /// <summary>Per-row timestamp deltas in whole seconds since <see cref="FirstTimestampTicks"/>.</summary>
        [Key(4)] public int[] TimestampDeltasSeconds { get; set; }
        /// <summary>Per-row snapshot buy price.</summary>
        [Key(5)] public double[] BuyPrice { get; set; }
        /// <summary>Per-row snapshot sell price.</summary>
        [Key(6)] public double[] SellPrice { get; set; }
        /// <summary>Per-row buy volume.</summary>
        [Key(7)] public long[] BuyVolume { get; set; }
        /// <summary>Per-row sell volume.</summary>
        [Key(8)] public long[] SellVolume { get; set; }
        /// <summary>Per-row buy moving week.</summary>
        [Key(9)] public long[] BuyMovingWeek { get; set; }
        /// <summary>Per-row sell moving week.</summary>
        [Key(10)] public long[] SellMovingWeek { get; set; }
        /// <summary>Per-row buy orders count.</summary>
        [Key(11)] public int[] BuyOrdersCount { get; set; }
        /// <summary>Per-row sell orders count.</summary>
        [Key(12)] public int[] SellOrdersCount { get; set; }
        /// <summary>Per-row max buy.</summary>
        [Key(13)] public float[] MaxBuy { get; set; }
        /// <summary>Per-row max sell.</summary>
        [Key(14)] public float[] MaxSell { get; set; }
        /// <summary>Per-row min buy.</summary>
        [Key(15)] public float[] MinBuy { get; set; }
        /// <summary>Per-row min sell.</summary>
        [Key(16)] public float[] MinSell { get; set; }
        /// <summary>Per-row sub-aggregation count.</summary>
        [Key(17)] public short[] Count { get; set; }
        /// <summary>Per-row serialised buy order book (MessagePack, null entries allowed). Format v2+.</summary>
        [Key(18)] public byte[][] SerialisedBuyOrders { get; set; }
        /// <summary>Per-row serialised sell order book (MessagePack, null entries allowed). Format v2+.</summary>
        [Key(19)] public byte[][] SerialisedSellOrders { get; set; }
        /// <summary>Per-row reference id. Format v2+.</summary>
        [Key(20)] public long[] ReferenceId { get; set; }
        /// <summary>Per-row timestamp deltas in milliseconds since <see cref="FirstTimestampTicks"/> (exceeds int over 30 days). Format v2+.</summary>
        [Key(21)] public long[] TimestampDeltasMs { get; set; }

        /// <summary>Format version written by <see cref="FromRows"/>.</summary>
        public const int CurrentFormatVersion = 2;

        /// <summary>Bucket epoch (must match the rest of the project).</summary>
        public static readonly DateTime Epoch = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        /// <summary>Bucket length in days.</summary>
        public const int BucketLengthDays = 30;

        /// <summary>Compute the 30-day bucket id for an instant.</summary>
        public static int GetBucketId(DateTime timestamp)
        {
            return (int)((timestamp - Epoch).TotalDays / BucketLengthDays);
        }

        /// <summary>UTC start (inclusive) of a bucket.</summary>
        public static DateTime BucketStart(int bucketId)
        {
            return Epoch.AddDays((double)bucketId * BucketLengthDays);
        }

        /// <summary>UTC end (exclusive) of a bucket.</summary>
        public static DateTime BucketEnd(int bucketId)
        {
            return BucketStart(bucketId + 1);
        }

        /// <summary>The two 15-day hot-table bucket ids that make up an archive (30-day) bucket.</summary>
        public static (int First, int Second) FifteenDayBucketIds(int archiveBucketId)
        {
            return (archiveBucketId * 2, archiveBucketId * 2 + 1);
        }

        private static long MsKey(DateTime t) => t.Ticks / TimeSpan.TicksPerMillisecond;

        /// <summary>
        /// True if this block can stand in for <paramref name="rows"/>: every row's timestamp (ms precision)
        /// is present, and the block is v2+ unless none of the source rows carries an order book
        /// (so v1 blobs get rewritten once order books exist).
        /// </summary>
        public bool CoversRows(IReadOnlyList<AggregatedQuickStatus> rows)
        {
            if (rows == null || rows.Count == 0)
                return true;
            if (FormatVersion < 2 && rows.Any(r => r.SerialisedBuyOrders != null || r.SerialisedSellOrders != null))
                return false;
            var have = new HashSet<long>();
            foreach (var r in ToRows())
                have.Add(MsKey(r.TimeStamp));
            return rows.All(r => have.Contains(MsKey(r.TimeStamp)));
        }

        /// <summary>Structural equality used to verify a read-back block against the one that was written.</summary>
        public bool SameRows(ArchivedMinuteBlock other)
        {
            if (other == null)
                return false;
            if (FormatVersion != other.FormatVersion || FirstTimestampTicks != other.FirstTimestampTicks)
                return false;
            if (!ArrayEq(TimestampDeltasSeconds, other.TimestampDeltasSeconds) || !ArrayEq(TimestampDeltasMs, other.TimestampDeltasMs))
                return false;
            if (!ArrayEq(ReferenceId, other.ReferenceId))
                return false;
            return BookLengthsEq(SerialisedBuyOrders, other.SerialisedBuyOrders) && BookLengthsEq(SerialisedSellOrders, other.SerialisedSellOrders);
        }

        private static bool ArrayEq<T>(T[] a, T[] b) where T : IEquatable<T>
        {
            if (a == null || b == null)
                return a == null && b == null;
            return a.AsSpan().SequenceEqual(b);
        }

        private static bool BookLengthsEq(byte[][] a, byte[][] b)
        {
            if (a == null || b == null)
                return a == null && b == null;
            if (a.Length != b.Length)
                return false;
            for (var i = 0; i < a.Length; i++)
                if ((a[i]?.Length ?? -1) != (b[i]?.Length ?? -1))
                    return false;
            return true;
        }

        /// <summary>Approximate decoded in-memory size, used to bill the in-process cache.</summary>
        public long EstimatedBytes()
        {
            var n = TimestampDeltasMs?.Length ?? TimestampDeltasSeconds?.Length ?? 0;
            // numeric columns: 2x double, 5x long, 2x int, 4x float, short, long ref, long ms delta, int sec delta
            const int perRowNumeric = 2 * 8 + 5 * 8 + 2 * 4 + 4 * 4 + 2 + 8 + 8 + 4;
            // byte[] header (24) + array slot (8) per order book entry
            const int perBookOverhead = 32;
            long total = 256 + (long)n * perRowNumeric;
            foreach (var books in new[] { SerialisedBuyOrders, SerialisedSellOrders })
            {
                if (books == null)
                    continue;
                foreach (var b in books)
                    total += perBookOverhead + (b?.Length ?? 0);
            }
            return total;
        }

        /// <summary>Pack a sequence of aggregated rows for one product into a column-oriented block.</summary>
        public static ArchivedMinuteBlock FromRows(string productId, int bucketId, IReadOnlyList<AggregatedQuickStatus> rows)
        {
            if (rows == null || rows.Count == 0)
                return null;
            var n = rows.Count;
            var first = rows[0].TimeStamp;
            var block = new ArchivedMinuteBlock
            {
                FormatVersion = CurrentFormatVersion,
                ProductId = productId,
                BucketId = bucketId,
                FirstTimestampTicks = first.Ticks,
                TimestampDeltasSeconds = new int[n],
                BuyPrice = new double[n],
                SellPrice = new double[n],
                BuyVolume = new long[n],
                SellVolume = new long[n],
                BuyMovingWeek = new long[n],
                SellMovingWeek = new long[n],
                BuyOrdersCount = new int[n],
                SellOrdersCount = new int[n],
                MaxBuy = new float[n],
                MaxSell = new float[n],
                MinBuy = new float[n],
                MinSell = new float[n],
                Count = new short[n],
                SerialisedBuyOrders = new byte[n][],
                SerialisedSellOrders = new byte[n][],
                ReferenceId = new long[n],
                TimestampDeltasMs = new long[n],
            };
            for (var i = 0; i < n; i++)
            {
                var r = rows[i];
                block.TimestampDeltasSeconds[i] = (int)((r.TimeStamp.Ticks - first.Ticks) / TimeSpan.TicksPerSecond);
                block.BuyPrice[i] = r.BuyPrice;
                block.SellPrice[i] = r.SellPrice;
                block.BuyVolume[i] = r.BuyVolume;
                block.SellVolume[i] = r.SellVolume;
                block.BuyMovingWeek[i] = r.BuyMovingWeek;
                block.SellMovingWeek[i] = r.SellMovingWeek;
                block.BuyOrdersCount[i] = r.BuyOrdersCount;
                block.SellOrdersCount[i] = r.SellOrdersCount;
                block.MaxBuy[i] = r.MaxBuy;
                block.MaxSell[i] = r.MaxSell;
                block.MinBuy[i] = r.MinBuy;
                block.MinSell[i] = r.MinSell;
                block.Count[i] = r.Count;
                block.SerialisedBuyOrders[i] = r.SerialisedBuyOrders;
                block.SerialisedSellOrders[i] = r.SerialisedSellOrders;
                block.ReferenceId[i] = r.ReferenceId;
                block.TimestampDeltasMs[i] = (r.TimeStamp.Ticks - first.Ticks) / TimeSpan.TicksPerMillisecond;
            }
            return block;
        }

        /// <summary>Materialize the column-oriented block back into individual rows.</summary>
        public IEnumerable<AggregatedQuickStatus> ToRows()
        {
            var useMs = TimestampDeltasMs != null;
            var deltas = useMs ? null : TimestampDeltasSeconds;
            if (!useMs && deltas == null)
                yield break;
            var n = useMs ? TimestampDeltasMs.Length : deltas.Length;
            for (var i = 0; i < n; i++)
            {
                yield return new AggregatedQuickStatus
                {
                    ProductId = ProductId,
                    TimeStamp = new DateTime(FirstTimestampTicks + (useMs ? TimestampDeltasMs[i] * TimeSpan.TicksPerMillisecond : (long)deltas[i] * TimeSpan.TicksPerSecond), DateTimeKind.Utc),
                    BuyPrice = BuyPrice[i],
                    SellPrice = SellPrice[i],
                    BuyVolume = BuyVolume[i],
                    SellVolume = SellVolume[i],
                    BuyMovingWeek = BuyMovingWeek[i],
                    SellMovingWeek = SellMovingWeek[i],
                    BuyOrdersCount = BuyOrdersCount[i],
                    SellOrdersCount = SellOrdersCount[i],
                    MaxBuy = MaxBuy[i],
                    MaxSell = MaxSell[i],
                    MinBuy = MinBuy[i],
                    MinSell = MinSell[i],
                    Count = Count[i],
                    SerialisedBuyOrders = SerialisedBuyOrders != null && i < SerialisedBuyOrders.Length ? SerialisedBuyOrders[i] : null,
                    SerialisedSellOrders = SerialisedSellOrders != null && i < SerialisedSellOrders.Length ? SerialisedSellOrders[i] : null,
                    ReferenceId = ReferenceId != null && i < ReferenceId.Length ? ReferenceId[i] : 0,
                };
            }
        }
    }
}
