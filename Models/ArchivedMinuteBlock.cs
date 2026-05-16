using System;
using System.Collections.Generic;
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
        /// <summary>Blob format version; bump on any breaking schema change.</summary>
        [Key(0)] public int FormatVersion { get; set; } = 1;
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

        /// <summary>Pack a sequence of aggregated rows for one product into a column-oriented block.</summary>
        public static ArchivedMinuteBlock FromRows(string productId, int bucketId, IReadOnlyList<AggregatedQuickStatus> rows)
        {
            if (rows == null || rows.Count == 0)
                return null;
            var n = rows.Count;
            var first = rows[0].TimeStamp;
            var block = new ArchivedMinuteBlock
            {
                FormatVersion = 1,
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
            }
            return block;
        }

        /// <summary>Materialize the column-oriented block back into individual rows.</summary>
        public IEnumerable<AggregatedQuickStatus> ToRows()
        {
            if (TimestampDeltasSeconds == null)
                yield break;
            for (var i = 0; i < TimestampDeltasSeconds.Length; i++)
            {
                yield return new AggregatedQuickStatus
                {
                    ProductId = ProductId,
                    TimeStamp = new DateTime(FirstTimestampTicks + (long)TimestampDeltasSeconds[i] * TimeSpan.TicksPerSecond, DateTimeKind.Utc),
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
                };
            }
        }
    }
}
