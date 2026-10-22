using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Sky.SkyBazaar.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Coflnet.Sky.SkyAuctionTracker.Services
{
    /// <summary>Tunables for one archive sweep.</summary>
    internal sealed record SweepOptions(
        int ClosedForDays,
        int RetentionHotDays,
        int DeleteMarginDays,
        bool DeleteEnabled,
        bool LegacyDropped,
        int MaxDeletesPerSweep);

    /// <summary>Result of processing one (product, 30-day bucket) pair.</summary>
    internal enum BucketOutcome
    {
        /// <summary>Marker says nothing is left to do.</summary>
        Skipped,
        /// <summary>No rows in either table.</summary>
        Empty,
        /// <summary>A blob was written, verified and marked.</summary>
        Archived,
        /// <summary>An existing blob already covered the rows (verified, marked).</summary>
        AlreadyArchived,
        /// <summary>Hot partitions were removed (or there was nothing hot to remove).</summary>
        Deleted,
        /// <summary>Write or read-back did not verify; nothing was marked or deleted.</summary>
        VerifyFailed,
        /// <summary>Deleting the hot partitions failed; marker stays archived and is retried.</summary>
        Failed
    }

    /// <summary>
    /// Background service that archives closed 30-day windows of 5-minute aggregated data
    /// (statistics plus the order-book snapshot) into cold-tier S3 blobs and, once enabled,
    /// trims the archived rows from the hot Scylla table.
    /// Per product-bucket (newest first) it merges the hot table <c>QuickStatusMin15Day</c> (both 15-day
    /// partitions of the bucket) with the legacy table <c>QuickStatusMin</c>, writes a v2 blob unless the
    /// stored blob already covers every row, re-reads the blob bypassing the cache and only after
    /// the read-back matches records the Redis marker <c>bazaar_archive:v2:{product}:{bucket}</c>
    /// (<c>archived|deleted|empty</c>). Hot partitions are deleted only when <c>HISTORY_ARCHIVE:DELETE_ARCHIVED</c>
    /// is true and the bucket ended before the hot window plus margin (never below the seconds-table
    /// retention bound, because the backfill could otherwise rewrite the rows). A sweep that finishes
    /// without any failure sets <c>bazaar_archive:v2:sweep_complete</c>, which is required before the
    /// legacy table may be dropped. Old v1 markers are ignored.
    /// </summary>
    public class HistoryArchiveService : BackgroundService
    {
        private readonly BazaarService bazaar;
        private readonly IBlobHistoryStore blobStore;
        private readonly IConnectionMultiplexer redis;
        private readonly IConfiguration config;
        private readonly ILogger<HistoryArchiveService> logger;
        private int deletesThisSweep;

        private static readonly Prometheus.Counter blocksArchived = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_blocks", "Cold-tier 30-day blocks archived");
        private static readonly Prometheus.Counter blocksEmpty = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_blocks_empty", "Cold-tier 30-day blocks marked empty (no data)");
        private static readonly Prometheus.Counter blocksFailed = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_blocks_failed", "Cold-tier 30-day archive attempts that failed");
        private static readonly Prometheus.Counter rowsArchived = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_rows", "Rows (after merging both tables) archived");
        private static readonly Prometheus.Counter blocksRewritten = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_blocks_rewritten", "Blobs written because the stored blob was missing or did not cover the rows");
        private static readonly Prometheus.Counter blocksVerified = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_blocks_verified", "Blobs verified by an uncached read-back");
        private static readonly Prometheus.Counter verifyFailures = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_verify_failures", "Blob read-back verifications that failed");
        private static readonly Prometheus.Counter hotRowsRead = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_hot_rows", "Rows read from QuickStatusMin15Day while archiving");
        private static readonly Prometheus.Counter legacyRowsRead = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_legacy_rows", "Rows read from QuickStatusMin while archiving");
        private static readonly Prometheus.Counter partitionsDeleted = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_partitions_deleted", "Hot 15-day partitions deleted after archiving");
        private static readonly Prometheus.Counter rowsDeleted = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_rows_deleted", "Hot rows deleted after archiving");
        private static readonly Prometheus.Counter deleteFailures = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_delete_failures", "Hot partition deletes that failed");
        private static readonly Prometheus.Counter blocksSkippedMarker = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_blocks_skipped_marker", "Buckets skipped because of a Redis marker");
        private static readonly Prometheus.Gauge sweepLastSuccess = Prometheus.Metrics.CreateGauge("sky_bazaar_archive_sweep_last_success_timestamp", "Unix time of the last archive sweep that finished without any failure");

        private const string MarkerPrefix = "bazaar_archive:v2:";
        private const string SweepCompleteKey = "bazaar_archive:v2:sweep_complete";
        private const string LegacyDroppedMarker = "bazaar_archive:legacy_dropped";

        /// <summary>DI constructor.</summary>
        public HistoryArchiveService(
            BazaarService bazaar,
            IBlobHistoryStore blobStore,
            IConnectionMultiplexer redis,
            IConfiguration config,
            ILogger<HistoryArchiveService> logger)
        {
            this.bazaar = bazaar;
            this.blobStore = blobStore;
            this.redis = redis;
            this.config = config;
            this.logger = logger;
        }

        /// <inheritdoc/>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            if (!bool.TryParse(config["HISTORY_ARCHIVE:ENABLED"], out var enabled) || !enabled)
                return;
            if (!blobStore.IsEnabled)
            {
                logger.LogWarning("HISTORY_ARCHIVE:ENABLED=true but S3 cold-tier is not configured; archive service will not run.");
                return;
            }

            var startDelaySeconds = ConfigInt("HISTORY_ARCHIVE:START_DELAY_SECONDS", 60);
            var oldestBucketId = ConfigInt("HISTORY_ARCHIVE:OLDEST_BUCKET_ID", 0);
            var emptyStreakLimit = ConfigInt("HISTORY_ARCHIVE:EMPTY_STREAK_LIMIT", 6);
            var perProductPauseMs = ConfigInt("HISTORY_ARCHIVE:PER_PRODUCT_PAUSE_MS", 250);
            var perBucketPauseMs = ConfigInt("HISTORY_ARCHIVE:PER_BUCKET_PAUSE_MS", 100);
            var parallelProducts = Math.Max(1, ConfigInt("HISTORY_ARCHIVE:PARALLEL_PRODUCTS", 1));
            var idleBetweenSweepsSeconds = ConfigInt("HISTORY_ARCHIVE:IDLE_BETWEEN_SWEEPS_SECONDS", 3600);

            if (startDelaySeconds > 0)
                await Task.Delay(TimeSpan.FromSeconds(startDelaySeconds), stoppingToken).ConfigureAwait(false);

            await MaybeDropLegacyTableAsync(stoppingToken).ConfigureAwait(false);

            var startOptions = ReadOptions();
            logger.LogInformation("Starting history archive sweeps; closedForDays={ClosedForDays} oldestBucket={Oldest} deleteEnabled={Delete} parallelProducts={Parallel}", startOptions.ClosedForDays, oldestBucketId, startOptions.DeleteEnabled, parallelProducts);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var products = GetProducts();
                    if (products.Count == 0)
                    {
                        logger.LogInformation("Archive: no known products yet, waiting.");
                        await Task.Delay(TimeSpan.FromMinutes(2), stoppingToken).ConfigureAwait(false);
                        continue;
                    }

                    var options = ReadOptions();
                    var now = DateTime.UtcNow;
                    var newestCold = NewestClosedBucket(now, options.ClosedForDays);
                    var db = redis.GetDatabase();
                    Interlocked.Exchange(ref deletesThisSweep, 0);

                    var problems = await ArchiveProductsAsync(products, parallelProducts, newestCold, oldestBucketId, emptyStreakLimit, perBucketPauseMs, perProductPauseMs, now, options, db, stoppingToken).ConfigureAwait(false);

                    if (stoppingToken.IsCancellationRequested)
                        break;
                    if (problems == 0)
                    {
                        await db.StringSetAsync(SweepCompleteKey, DateTime.UtcNow.ToString("O")).ConfigureAwait(false);
                        sweepLastSuccess.Set(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
                    }
                    logger.LogInformation("Archive sweep complete (problems={Problems}); idling for {Idle}s", problems, idleBetweenSweepsSeconds);
                    await Task.Delay(TimeSpan.FromSeconds(idleBetweenSweepsSeconds), stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Archive sweep failed, retrying after backoff");
                    await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Archives the products with at most <paramref name="parallelProducts"/> in flight, a worker pool draining a shared queue
        /// (queue order = product order). Returns the total number of problems. Cancellation propagates; any other exception of a
        /// worker is counted as a problem and does not stop the other workers.
        /// </summary>
        private async Task<int> ArchiveProductsAsync(
            IReadOnlyList<string> products,
            int parallelProducts,
            int newestCold,
            int oldestBucketId,
            int emptyStreakLimit,
            int perBucketPauseMs,
            int perProductPauseMs,
            DateTime now,
            SweepOptions options,
            IDatabase db,
            CancellationToken stoppingToken)
        {
            var queue = new ConcurrentQueue<string>(products);
            var problems = 0;
            var workerCount = Math.Max(1, Math.Min(parallelProducts, products.Count));
            async Task Worker()
            {
                while (!stoppingToken.IsCancellationRequested && queue.TryDequeue(out var product))
                {
                    try
                    {
                        var found = await ArchiveProductAsync(product, newestCold, oldestBucketId, emptyStreakLimit, perBucketPauseMs, now, options, db, stoppingToken).ConfigureAwait(false);
                        Interlocked.Add(ref problems, found);
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception ex)
                    {
                        Interlocked.Increment(ref problems);
                        logger.LogError(ex, "Archive: worker failed on {Product}; continuing with the next product", product);
                    }
                    if (perProductPauseMs > 0)
                        await Task.Delay(perProductPauseMs, stoppingToken).ConfigureAwait(false);
                }
            }
            await Task.WhenAll(Enumerable.Range(0, workerCount).Select(_ => Task.Run(Worker, CancellationToken.None))).ConfigureAwait(false);
            return Volatile.Read(ref problems);
        }

        /// <summary>Atomically reserves one delete against the per-sweep cap (0 = unlimited).</summary>
        private bool TryReserveDelete(int max)
        {
            if (max <= 0)
            {
                Interlocked.Increment(ref deletesThisSweep);
                return true;
            }
            while (true)
            {
                var current = Volatile.Read(ref deletesThisSweep);
                if (current >= max)
                    return false;
                if (Interlocked.CompareExchange(ref deletesThisSweep, current + 1, current) == current)
                    return true;
            }
        }

        /// <summary>Waits before retrying a failed bucket (seam for tests).</summary>
        protected virtual Task RetryBackoffAsync(TimeSpan delay, CancellationToken cancellationToken) => Task.Delay(delay, cancellationToken);

        private SweepOptions ReadOptions()
        {
            return new SweepOptions(
                ConfigInt("HISTORY_ARCHIVE:CLOSED_FOR_DAYS", 7),
                ConfigInt("HISTORY_ARCHIVE:RETENTION_HOT_DAYS", 30),
                ConfigInt("HISTORY_ARCHIVE:DELETE_MARGIN_DAYS", 31),
                ConfigBool("HISTORY_ARCHIVE:DELETE_ARCHIVED"),
                ConfigBool("HISTORY_ARCHIVE:DROP_LEGACY_TABLE"),
                ConfigInt("HISTORY_ARCHIVE:MAX_DELETES_PER_SWEEP", 0));
        }

        /// <summary>Largest bucket whose end is not after <c>now - closedForDays</c> (a bucket still open is never archived).</summary>
        internal static int NewestClosedBucket(DateTime now, int closedForDays)
        {
            var cutoff = now.AddDays(-closedForDays);
            var b = ArchivedMinuteBlock.GetBucketId(cutoff);
            while (ArchivedMinuteBlock.BucketEnd(b) > cutoff)
                b--;
            while (ArchivedMinuteBlock.BucketEnd(b + 1) <= cutoff)
                b++;
            return b;
        }

        /// <summary>
        /// True if the hot partitions of the bucket may be deleted: the bucket ended before the hot window plus
        /// margin, and in any case before the seconds-table retention (+1 day) so the startup backfill can not rewrite them.
        /// </summary>
        internal static bool IsDeleteEligible(int bucketId, DateTime now, SweepOptions o)
        {
            var days = Math.Max(o.RetentionHotDays + o.DeleteMarginDays, BazaarService.SECONDS_RETENTION_SECONDS / 86400 + 1);
            return ArchivedMinuteBlock.BucketEnd(bucketId) < now.AddDays(-days);
        }

        /// <summary>
        /// Merge hot and legacy rows keyed on the 5-minute window. Prefers the higher <c>Count</c>, then the row
        /// carrying order books, then the hot row. Result is sorted ascending by timestamp.
        /// </summary>
        internal static List<AggregatedQuickStatus> MergeForArchive(IEnumerable<AggregatedQuickStatus> hot, IEnumerable<AggregatedQuickStatus> legacy)
        {
            const long window = 5 * TimeSpan.TicksPerMinute;
            var chosen = new Dictionary<long, AggregatedQuickStatus>();
            foreach (var row in (hot ?? Enumerable.Empty<AggregatedQuickStatus>()).Concat(legacy ?? Enumerable.Empty<AggregatedQuickStatus>()))
            {
                var key = row.TimeStamp.Ticks / window;
                if (!chosen.TryGetValue(key, out var current) || IsBetter(row, current))
                    chosen[key] = row;
            }
            var result = chosen.Values.ToList();
            result.Sort((a, b) => a.TimeStamp.CompareTo(b.TimeStamp));
            return result;
        }

        private static bool IsBetter(AggregatedQuickStatus candidate, AggregatedQuickStatus current)
        {
            if (candidate.Count != current.Count)
                return candidate.Count > current.Count;
            return HasBooks(candidate) && !HasBooks(current);
        }

        private static bool HasBooks(AggregatedQuickStatus r) => r.SerialisedBuyOrders != null || r.SerialisedSellOrders != null;

        internal static string MarkerKey(string productId, int bucketId) => MarkerPrefix + productId + ":" + bucketId;

        /// <summary>Parse a marker value into its kind (<c>archived|deleted|empty</c>) and row count; (null, 0) when unknown.</summary>
        internal static (string Kind, int Rows) ParseMarker(string value)
        {
            if (string.IsNullOrEmpty(value))
                return (null, 0);
            var parts = value.Split(':', 3);
            switch (parts[0])
            {
                case "empty":
                    return ("empty", 0);
                case "archived":
                case "deleted":
                    return (parts[0], parts.Length > 1 && int.TryParse(parts[1], out var rows) ? rows : 0);
                default:
                    return (null, 0);
            }
        }

        private static string Iso() => DateTime.UtcNow.ToString("O");

        /// <summary>Hot rows of one bucket: both 15-day partitions of QuickStatusMin15Day.</summary>
        protected virtual async Task<List<AggregatedQuickStatus>> LoadHotRowsAsync(string productId, int bucketId, DateTime start, DateTime end)
        {
            var table = BazaarService.GetSplitMinutesTable(bazaar.Session);
            var (first, second) = ArchivedMinuteBlock.FifteenDayBucketIds(bucketId);
            var collected = new List<AggregatedQuickStatus>();
            foreach (var partition in new[] { first, second })
            {
                var rows = await table
                    .Where(r => r.ProductId == productId && r.BucketId == partition && r.TimeStamp >= start && r.TimeStamp < end)
                    .ExecuteAsync().ConfigureAwait(false);
                collected.AddRange(rows);
            }
            collected.Sort((a, b) => a.TimeStamp.CompareTo(b.TimeStamp));
            return collected;
        }

        /// <summary>Legacy rows of one bucket; empty without querying once the legacy table is dropped.</summary>
        protected virtual async Task<List<AggregatedQuickStatus>> LoadLegacyRowsAsync(string productId, DateTime bucketStart, DateTime bucketEnd)
        {
            var collected = new List<AggregatedQuickStatus>();
            if (ConfigBool("HISTORY_ARCHIVE:DROP_LEGACY_TABLE"))
                return collected;
            var table = BazaarService.GetLegacySplitMinutesTable(bazaar.Session);
            foreach (var q in QuartersOverlapping(bucketStart, bucketEnd))
            {
                var rows = await table
                    .Where(r => r.ProductId == productId && r.QuaterId == q && r.TimeStamp >= bucketStart && r.TimeStamp < bucketEnd)
                    .ExecuteAsync().ConfigureAwait(false);
                collected.AddRange(rows);
            }
            collected.Sort((a, b) => a.TimeStamp.CompareTo(b.TimeStamp));
            return collected;
        }

        /// <summary>Delete both 15-day hot partitions of the bucket (idempotent).</summary>
        protected virtual async Task DeleteHotPartitionsAsync(string productId, int bucketId, CancellationToken cancellationToken)
        {
            var (first, second) = ArchivedMinuteBlock.FifteenDayBucketIds(bucketId);
            foreach (var partition in new[] { first, second })
            {
                cancellationToken.ThrowIfCancellationRequested();
                var statement = new SimpleStatement("DELETE FROM QuickStatusMin15Day WHERE ProductId = ? AND BucketId = ?", productId, partition)
                    .SetConsistencyLevel(ConsistencyLevel.Quorum);
                await bazaar.Session.ExecuteAsync(statement).ConfigureAwait(false);
            }
        }

        /// <summary>Products to archive.</summary>
        protected virtual IReadOnlyList<string> GetProducts() => bazaar.GetKnownProductIds();

        /// <summary>Archive (and optionally trim) one product-bucket. Order: marker check, load, write-if-needed, verify, mark, delete.</summary>
        internal async Task<BucketOutcome> ProcessBucketAsync(string productId, int bucketId, DateTime now, SweepOptions options, IDatabase db, CancellationToken cancellationToken)
        {
            var markerKey = MarkerKey(productId, bucketId);
            var eligible = IsDeleteEligible(bucketId, now, options);
            var (kind, _) = ParseMarker((string)await db.StringGetAsync(markerKey).ConfigureAwait(false));
            if (kind == "deleted" || ((kind == "archived" || kind == "empty") && (!eligible || !options.DeleteEnabled)))
            {
                blocksSkippedMarker.Inc();
                return BucketOutcome.Skipped;
            }

            var bucketStart = ArchivedMinuteBlock.BucketStart(bucketId);
            var bucketEnd = ArchivedMinuteBlock.BucketEnd(bucketId);
            var hot = await LoadHotRowsAsync(productId, bucketId, bucketStart, bucketEnd).ConfigureAwait(false);
            var legacy = options.LegacyDropped
                ? new List<AggregatedQuickStatus>()
                : await LoadLegacyRowsAsync(productId, bucketStart, bucketEnd).ConfigureAwait(false);
            hotRowsRead.Inc(hot.Count);
            legacyRowsRead.Inc(legacy.Count);
            var merged = MergeForArchive(hot, legacy);

            if (merged.Count == 0)
            {
                await db.StringSetAsync(markerKey, (eligible ? "deleted:0:" : "empty:") + Iso()).ConfigureAwait(false);
                blocksEmpty.Inc();
                return BucketOutcome.Empty;
            }
            rowsArchived.Inc(merged.Count);

            var outcome = BucketOutcome.AlreadyArchived;
            var existing = await blobStore.ReadBlobUncachedAsync(productId, bucketId, cancellationToken).ConfigureAwait(false);
            if (existing == null || !existing.CoversRows(merged))
            {
                var block = ArchivedMinuteBlock.FromRows(productId, bucketId, merged);
                await blobStore.WriteBlobAsync(block, cancellationToken).ConfigureAwait(false);
                blocksRewritten.Inc();
                var readBack = await blobStore.ReadBlobUncachedAsync(productId, bucketId, cancellationToken).ConfigureAwait(false);
                if (readBack == null || !block.SameRows(readBack))
                {
                    verifyFailures.Inc();
                    logger.LogWarning("Archive: read-back verification failed for {Product} bucket {Bucket}; leaving marker and hot data untouched.", productId, bucketId);
                    return BucketOutcome.VerifyFailed;
                }
                outcome = BucketOutcome.Archived;
            }
            blocksVerified.Inc();

            await db.StringSetAsync(markerKey, $"archived:{merged.Count}:{Iso()}").ConfigureAwait(false);
            blocksArchived.Inc();

            if (!options.DeleteEnabled || !eligible)
                return outcome;

            if (hot.Count == 0)
            {
                await db.StringSetAsync(markerKey, $"deleted:0:{Iso()}").ConfigureAwait(false);
                return BucketOutcome.Deleted;
            }
            if (!TryReserveDelete(options.MaxDeletesPerSweep))
                return outcome;
            try
            {
                await DeleteHotPartitionsAsync(productId, bucketId, cancellationToken).ConfigureAwait(false);
                partitionsDeleted.Inc(2);
                rowsDeleted.Inc(hot.Count);
                await db.StringSetAsync(markerKey, $"deleted:{hot.Count}:{Iso()}").ConfigureAwait(false);
                return BucketOutcome.Deleted;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                deleteFailures.Inc();
                logger.LogWarning(ex, "Archive: deleting hot partitions of {Product} bucket {Bucket} failed; will retry next sweep.", productId, bucketId);
                return BucketOutcome.Failed;
            }
        }

        /// <summary>Archives all buckets of a product, newest first. Returns the number of failures, verify failures and breaker trips.</summary>
        private async Task<int> ArchiveProductAsync(
            string productId,
            int newestColdBucket,
            int oldestBucket,
            int emptyStreakLimit,
            int perBucketPauseMs,
            DateTime now,
            SweepOptions options,
            IDatabase db,
            CancellationToken cancellationToken)
        {
            var consecutiveEmpty = 0;
            var consecutiveFailures = 0;
            var problems = 0;
            const int failureCircuitBreakerThreshold = 5;
            for (var bucketId = newestColdBucket; bucketId >= oldestBucket; bucketId--)
            {
                if (cancellationToken.IsCancellationRequested)
                    return problems;

                try
                {
                    var outcome = await ProcessBucketAsync(productId, bucketId, now, options, db, cancellationToken).ConfigureAwait(false);
                    if (outcome == BucketOutcome.VerifyFailed || outcome == BucketOutcome.Failed)
                        throw new InvalidOperationException("Bucket outcome " + outcome);

                    consecutiveFailures = 0;
                    if (outcome == BucketOutcome.Empty)
                    {
                        consecutiveEmpty++;
                        if (consecutiveEmpty >= emptyStreakLimit)
                        {
                            logger.LogInformation("Archive: {Product} hit empty streak {Streak} at bucket {Bucket}; stopping descent.", productId, consecutiveEmpty, bucketId);
                            return problems;
                        }
                        continue;
                    }
                    consecutiveEmpty = 0;

                    if (outcome != BucketOutcome.Skipped && perBucketPauseMs > 0)
                        await Task.Delay(perBucketPauseMs, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return problems;
                }
                catch (Exception ex)
                {
                    blocksFailed.Inc();
                    problems++;
                    consecutiveFailures++;
                    logger.LogWarning(ex, "Archive: {Product} bucket {Bucket} failed (consecutive failures: {Failures})", productId, bucketId, consecutiveFailures);
                    // Marker is not advanced; will retry on next sweep.
                    if (consecutiveFailures >= failureCircuitBreakerThreshold)
                    {
                        logger.LogWarning("Archive: {Product} tripped circuit breaker after {Failures} consecutive failures; skipping the rest of this product for this sweep.", productId, consecutiveFailures);
                        return problems;
                    }
                    // Exponential backoff capped at 2 minutes: 5s, 10s, 20s, 40s, 80s.
                    var backoffSeconds = Math.Min(120, 5 * (int)Math.Pow(2, consecutiveFailures - 1));
                    await RetryBackoffAsync(TimeSpan.FromSeconds(backoffSeconds), cancellationToken).ConfigureAwait(false);
                }
            }
            return problems;
        }

        private static IEnumerable<short> QuartersOverlapping(DateTime start, DateTime end)
        {
            var first = SplitAggregatedQuickStatus.GetQuarterId(start);
            var last = SplitAggregatedQuickStatus.GetQuarterId(end.AddTicks(-1));
            for (var q = first; q <= last; q++)
                yield return q;
        }

        /// <summary>
        /// Drop the legacy <c>QuickStatusMin</c> table iff <c>HISTORY_ARCHIVE:DROP_LEGACY_TABLE=true</c>,
        /// a complete sweep has been recorded (<c>sweep_complete</c>) and it has not been dropped before.
        /// <see cref="BazaarService.LoadMinuteHistory"/> reads the same flag and skips the legacy
        /// fallback query when set so a dropped table does not cause read failures.
        /// </summary>
        private async Task MaybeDropLegacyTableAsync(CancellationToken cancellationToken)
        {
            if (!ConfigBool("HISTORY_ARCHIVE:DROP_LEGACY_TABLE"))
                return;
            var db = redis.GetDatabase();
            if (await db.KeyExistsAsync(LegacyDroppedMarker).ConfigureAwait(false))
                return;
            if (!await db.KeyExistsAsync(SweepCompleteKey).ConfigureAwait(false))
            {
                logger.LogWarning("HISTORY_ARCHIVE:DROP_LEGACY_TABLE=true but no complete archive sweep is recorded ({Key}); not dropping the legacy table.", SweepCompleteKey);
                return;
            }
            logger.LogWarning("HISTORY_ARCHIVE:DROP_LEGACY_TABLE=true and no prior drop marker; dropping legacy table QuickStatusMin now.");
            try
            {
                await bazaar.Session.ExecuteAsync(new SimpleStatement("DROP TABLE IF EXISTS QuickStatusMin")).ConfigureAwait(false);
                await db.StringSetAsync(LegacyDroppedMarker, DateTime.UtcNow.ToString("O")).ConfigureAwait(false);
                logger.LogWarning("Legacy table QuickStatusMin dropped successfully.");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to drop legacy table QuickStatusMin; will retry on next startup.");
            }
        }

        private int ConfigInt(string key, int fallback)
        {
            return int.TryParse(config[key], out var v) ? v : fallback;
        }

        private bool ConfigBool(string key)
        {
            return bool.TryParse(config[key], out var v) && v;
        }
    }
}
