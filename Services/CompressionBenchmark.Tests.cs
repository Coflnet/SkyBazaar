using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Coflnet.Sky.SkyBazaar.Models;
using MessagePack;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Coflnet.Sky.SkyAuctionTracker.Services
{
    /// <summary>
    /// One-off benchmark comparing compression options for the cold-tier archive blob.
    /// Marked Explicit so it does not run on every CI sweep; run on demand with:
    ///   dotnet test --filter "FullyQualifiedName~CompressionBenchmark"
    /// </summary>
    [TestFixture]
    public class CompressionBenchmark
    {
        [Test, Explicit("Benchmark only \u2014 run with: dotnet test --filter FullyQualifiedName~CompressionBenchmark")]
        public void CompareAlgorithms()
        {
            var block = BuildRepresentative30DayBlock();
            using var msgpackStream = new MemoryStream();
            MessagePackSerializer.Serialize(msgpackStream, block);
            var rawMsgpack = msgpackStream.ToArray();

            TestContext.Out.WriteLine($"Rows                 : {block.TimestampDeltasSeconds.Length}");
            TestContext.Out.WriteLine($"MessagePack raw size : {rawMsgpack.Length,10} bytes");
            TestContext.Out.WriteLine($"{"Algorithm",-32} {"Size",10}  {"Ratio",8}  {"Encode",10}  {"Decode",10}");
            TestContext.Out.WriteLine(new string('-', 78));

            Run(rawMsgpack, "Deflate Optimal",       s => new DeflateStream(s, CompressionLevel.Optimal, leaveOpen: true),       s => new DeflateStream(s, CompressionMode.Decompress));
            Run(rawMsgpack, "Deflate SmallestSize",  s => new DeflateStream(s, CompressionLevel.SmallestSize, leaveOpen: true),  s => new DeflateStream(s, CompressionMode.Decompress));
            Run(rawMsgpack, "Gzip Optimal",          s => new GZipStream(s, CompressionLevel.Optimal, leaveOpen: true),          s => new GZipStream(s, CompressionMode.Decompress));
            Run(rawMsgpack, "Gzip SmallestSize",     s => new GZipStream(s, CompressionLevel.SmallestSize, leaveOpen: true),     s => new GZipStream(s, CompressionMode.Decompress));
            Run(rawMsgpack, "Brotli Fastest",        s => new BrotliStream(s, CompressionLevel.Fastest, leaveOpen: true),        s => new BrotliStream(s, CompressionMode.Decompress));
            Run(rawMsgpack, "Brotli Optimal",        s => new BrotliStream(s, CompressionLevel.Optimal, leaveOpen: true),        s => new BrotliStream(s, CompressionMode.Decompress));
            Run(rawMsgpack, "Brotli SmallestSize",   s => new BrotliStream(s, CompressionLevel.SmallestSize, leaveOpen: true),   s => new BrotliStream(s, CompressionMode.Decompress));
        }

        [Test, Explicit("Benchmark only \u2014 run with: dotnet test --filter FullyQualifiedName~CompressionBenchmark")]
        public void CompareAlgorithms_CookieDataset()
        {
            // Uses the real cookie.json sample (~25h of BOOSTER_COOKIE snapshots) as a representative
            // dataset. We discard order books (the cold-tier archive only stores aggregated fields)
            // and pack the remaining snapshots into a single ArchivedMinuteBlock to measure how
            // each algorithm performs on real-world field distributions.
            var path = LocateCookieJson();
            if (path == null)
            {
                Assert.Ignore("cookie.json not found in repo root or working directory; skipping real-data benchmark.");
                return;
            }
            var snapshots = JsonConvert.DeserializeObject<List<CookieRow>>(File.ReadAllText(path));
            Assert.That(snapshots, Is.Not.Null.And.Not.Empty, "cookie.json deserialized to empty list");
            snapshots = snapshots.OrderBy(s => s.TimeStamp).ToList();

            var rows = snapshots.Select(s => new AggregatedQuickStatus
            {
                ProductId = s.ProductId,
                TimeStamp = s.TimeStamp,
                BuyPrice = s.BuyPrice,
                SellPrice = s.SellPrice,
                BuyVolume = s.BuyVolume,
                SellVolume = s.SellVolume,
                BuyMovingWeek = s.BuyMovingWeek,
                SellMovingWeek = s.SellMovingWeek,
                BuyOrdersCount = s.BuyOrdersCount,
                SellOrdersCount = s.SellOrdersCount,
                MaxBuy = (float)s.BuyPrice,
                MaxSell = (float)s.SellPrice,
                MinBuy = (float)s.BuyPrice,
                MinSell = (float)s.SellPrice,
                Count = 1,
            }).ToList();

            var productId = rows[0].ProductId ?? "BOOSTER_COOKIE";
            var bucketId = ArchivedMinuteBlock.GetBucketId(rows[0].TimeStamp);
            var block = ArchivedMinuteBlock.FromRows(productId, bucketId, rows);
            using var msgpackStream = new MemoryStream();
            MessagePackSerializer.Serialize(msgpackStream, block);
            var rawMsgpack = msgpackStream.ToArray();

            var span = rows[^1].TimeStamp - rows[0].TimeStamp;
            TestContext.Out.WriteLine($"Source               : {path}");
            TestContext.Out.WriteLine($"Product              : {productId}");
            TestContext.Out.WriteLine($"Rows                 : {rows.Count}");
            TestContext.Out.WriteLine($"Time span            : {span}");
            TestContext.Out.WriteLine($"MessagePack raw size : {rawMsgpack.Length,10} bytes ({rawMsgpack.Length / (double)rows.Count,5:F1} B/row)");
            TestContext.Out.WriteLine($"{"Algorithm",-32} {"Size",10}  {"Ratio",8}  {"Encode",10}  {"Decode",10}");
            TestContext.Out.WriteLine(new string('-', 78));

            Run(rawMsgpack, "Deflate Optimal",       s => new DeflateStream(s, CompressionLevel.Optimal, leaveOpen: true),       s => new DeflateStream(s, CompressionMode.Decompress));
            Run(rawMsgpack, "Deflate SmallestSize",  s => new DeflateStream(s, CompressionLevel.SmallestSize, leaveOpen: true),  s => new DeflateStream(s, CompressionMode.Decompress));
            Run(rawMsgpack, "Gzip Optimal",          s => new GZipStream(s, CompressionLevel.Optimal, leaveOpen: true),          s => new GZipStream(s, CompressionMode.Decompress));
            Run(rawMsgpack, "Gzip SmallestSize",     s => new GZipStream(s, CompressionLevel.SmallestSize, leaveOpen: true),     s => new GZipStream(s, CompressionMode.Decompress));
            Run(rawMsgpack, "Brotli Fastest",        s => new BrotliStream(s, CompressionLevel.Fastest, leaveOpen: true),        s => new BrotliStream(s, CompressionMode.Decompress));
            Run(rawMsgpack, "Brotli Optimal",        s => new BrotliStream(s, CompressionLevel.Optimal, leaveOpen: true),        s => new BrotliStream(s, CompressionMode.Decompress));
            Run(rawMsgpack, "Brotli SmallestSize",   s => new BrotliStream(s, CompressionLevel.SmallestSize, leaveOpen: true),   s => new BrotliStream(s, CompressionMode.Decompress));
        }

        private static string LocateCookieJson()
        {
            var candidates = new[]
            {
                Path.Combine(TestContext.CurrentContext.TestDirectory, "cookie.json"),
                Path.Combine(TestContext.CurrentContext.TestDirectory, "..", "..", "..", "cookie.json"),
                Path.Combine(Directory.GetCurrentDirectory(), "cookie.json"),
            };
            foreach (var c in candidates)
            {
                var full = Path.GetFullPath(c);
                if (File.Exists(full)) return full;
            }
            return null;
        }

        // Minimal DTO for cookie.json (field names match the file, not StorageQuickStatus's
        // JsonProperty attributes, which differ slightly).
        private class CookieRow
        {
            [JsonProperty("productId")] public string ProductId { get; set; }
            [JsonProperty("buyPrice")] public double BuyPrice { get; set; }
            [JsonProperty("buyVolume")] public long BuyVolume { get; set; }
            [JsonProperty("buyMovingWeek")] public long BuyMovingWeek { get; set; }
            [JsonProperty("buyOrdersCount")] public int BuyOrdersCount { get; set; }
            [JsonProperty("sellPrice")] public double SellPrice { get; set; }
            [JsonProperty("sellVolume")] public long SellVolume { get; set; }
            [JsonProperty("sellMovingWeek")] public long SellMovingWeek { get; set; }
            [JsonProperty("sellOrdersCount")] public int SellOrdersCount { get; set; }
            [JsonProperty("timeStamp")] public DateTime TimeStamp { get; set; }
        }

        private static void Run(byte[] input, string name, Func<Stream, Stream> encoder, Func<Stream, Stream> decoder)
        {
            byte[] compressed;
            var swEnc = Stopwatch.StartNew();
            using (var ms = new MemoryStream())
            {
                using (var enc = encoder(ms))
                    enc.Write(input, 0, input.Length);
                compressed = ms.ToArray();
            }
            swEnc.Stop();

            var swDec = Stopwatch.StartNew();
            using (var compressedStream = new MemoryStream(compressed))
            using (var dec = decoder(compressedStream))
            using (var roundTrip = new MemoryStream())
            {
                dec.CopyTo(roundTrip);
                Assert.That(roundTrip.Length, Is.EqualTo(input.Length), $"{name} round-trip size mismatch");
            }
            swDec.Stop();

            var ratio = (double)compressed.Length / input.Length;
            TestContext.Out.WriteLine($"{name,-32} {compressed.Length,10}  {ratio,7:P2}  {swEnc.ElapsedMilliseconds,8} ms  {swDec.ElapsedMilliseconds,8} ms");
        }

        private static ArchivedMinuteBlock BuildRepresentative30DayBlock()
        {
            // 30 days at 5-minute granularity = 8640 rows.
            const int rows = 30 * 24 * 12;
            var rng = new Random(42);
            var block = new ArchivedMinuteBlock
            {
                FormatVersion = 1,
                ProductId = "ENCHANTED_DIAMOND",
                BucketId = 50,
                FirstTimestampTicks = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks,
                TimestampDeltasSeconds = new int[rows],
                BuyPrice = new double[rows],
                SellPrice = new double[rows],
                BuyVolume = new long[rows],
                SellVolume = new long[rows],
                BuyMovingWeek = new long[rows],
                SellMovingWeek = new long[rows],
                BuyOrdersCount = new int[rows],
                SellOrdersCount = new int[rows],
                MaxBuy = new float[rows],
                MaxSell = new float[rows],
                MinBuy = new float[rows],
                MinSell = new float[rows],
                Count = new short[rows],
            };

            // Simulate a slowly-drifting market: a base price that walks plus 5-min noise.
            double buy = 12_345.67, sell = 12_300.50;
            long buyMw = 4_500_000, sellMw = 4_400_000;
            for (var i = 0; i < rows; i++)
            {
                block.TimestampDeltasSeconds[i] = i * 300; // 5 minutes
                buy += (rng.NextDouble() - 0.5) * 5;
                sell += (rng.NextDouble() - 0.5) * 5;
                buyMw += rng.Next(-500, 500);
                sellMw += rng.Next(-500, 500);
                block.BuyPrice[i] = buy;
                block.SellPrice[i] = sell;
                block.BuyVolume[i] = 80_000 + rng.Next(-2000, 2000);
                block.SellVolume[i] = 60_000 + rng.Next(-2000, 2000);
                block.BuyMovingWeek[i] = buyMw;
                block.SellMovingWeek[i] = sellMw;
                block.BuyOrdersCount[i] = 320 + rng.Next(-20, 20);
                block.SellOrdersCount[i] = 280 + rng.Next(-20, 20);
                block.MaxBuy[i] = (float)(buy + rng.NextDouble() * 2);
                block.MaxSell[i] = (float)(sell + rng.NextDouble() * 2);
                block.MinBuy[i] = (float)(buy - rng.NextDouble() * 2);
                block.MinSell[i] = (float)(sell - rng.NextDouble() * 2);
                block.Count[i] = 5;
            }
            return block;
        }
    }
}
