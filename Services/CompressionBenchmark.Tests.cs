using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using Coflnet.Sky.SkyBazaar.Models;
using MessagePack;
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
