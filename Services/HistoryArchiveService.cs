using System;
using System.Collections.Generic;
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
    /// <summary>
    /// Background service that exports closed 30-day windows of 5-minute aggregated data
    /// from the legacy Scylla table <c>QuickStatusMin</c> into cold-tier S3 blobs.
    /// Iterates newest cold bucket first per product so that more frequently queried recent
    /// history becomes S3-available earlier in the migration. Idempotent via Redis markers.
    /// </summary>
    public class HistoryArchiveService : BackgroundService
    {
        private readonly BazaarService bazaar;
        private readonly IBlobHistoryStore blobStore;
        private readonly ConnectionMultiplexer redis;
        private readonly IConfiguration config;
        private readonly ILogger<HistoryArchiveService> logger;

        private static readonly Prometheus.Counter blocksArchived = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_blocks", "Cold-tier 30-day blocks archived");
        private static readonly Prometheus.Counter blocksEmpty = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_blocks_empty", "Cold-tier 30-day blocks marked empty (no legacy data)");
        private static readonly Prometheus.Counter blocksFailed = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_blocks_failed", "Cold-tier 30-day archive attempts that failed");
        private static readonly Prometheus.Counter rowsArchived = Prometheus.Metrics.CreateCounter("sky_bazaar_archive_rows", "Legacy rows read while archiving");

        /// <summary>DI constructor.</summary>
        public HistoryArchiveService(
            BazaarService bazaar,
            IBlobHistoryStore blobStore,
            ConnectionMultiplexer redis,
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
            var closedForDays = ConfigInt("HISTORY_ARCHIVE:CLOSED_FOR_DAYS", 7);
            var oldestBucketId = ConfigInt("HISTORY_ARCHIVE:OLDEST_BUCKET_ID", 0);
            var emptyStreakLimit = ConfigInt("HISTORY_ARCHIVE:EMPTY_STREAK_LIMIT", 6);
            var perProductPauseMs = ConfigInt("HISTORY_ARCHIVE:PER_PRODUCT_PAUSE_MS", 250);
            var perBucketPauseMs = ConfigInt("HISTORY_ARCHIVE:PER_BUCKET_PAUSE_MS", 100);
            var idleBetweenSweepsSeconds = ConfigInt("HISTORY_ARCHIVE:IDLE_BETWEEN_SWEEPS_SECONDS", 3600);

            if (startDelaySeconds > 0)
                await Task.Delay(TimeSpan.FromSeconds(startDelaySeconds), stoppingToken).ConfigureAwait(false);

            await MaybeDropLegacyTableAsync(stoppingToken).ConfigureAwait(false);

            logger.LogInformation("Starting history archive sweeps; closedForDays={ClosedForDays} oldestBucket={Oldest}", closedForDays, oldestBucketId);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var products = bazaar.GetKnownProductIds();
                    if (products.Count == 0)
                    {
                        logger.LogInformation("Archive: no known products yet, waiting.");
                        await Task.Delay(TimeSpan.FromMinutes(2), stoppingToken).ConfigureAwait(false);
                        continue;
                    }

                    var newestCold = ArchivedMinuteBlock.GetBucketId(DateTime.UtcNow.AddDays(-closedForDays));

                    foreach (var product in products)
                    {
                        if (stoppingToken.IsCancellationRequested)
                            break;
                        await ArchiveProductAsync(product, newestCold, oldestBucketId, emptyStreakLimit, perBucketPauseMs, stoppingToken).ConfigureAwait(false);
                        if (perProductPauseMs > 0)
                            await Task.Delay(perProductPauseMs, stoppingToken).ConfigureAwait(false);
                    }

                    logger.LogInformation("Archive sweep complete; idling for {Idle}s", idleBetweenSweepsSeconds);
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

        private async Task ArchiveProductAsync(
            string productId,
            int newestColdBucket,
            int oldestBucket,
            int emptyStreakLimit,
            int perBucketPauseMs,
            CancellationToken cancellationToken)
        {
            var db = redis.GetDatabase();
            var consecutiveEmpty = 0;
            var consecutiveFailures = 0;
            const int failureCircuitBreakerThreshold = 5;
            for (var bucketId = newestColdBucket; bucketId >= oldestBucket; bucketId--)
            {
                if (cancellationToken.IsCancellationRequested)
                    return;

                var markerKey = MarkerKey(productId, bucketId);
                if (await db.KeyExistsAsync(markerKey).ConfigureAwait(false))
                {
                    // Markers are immutable; an archived bucket stays archived.
                    // To force re-archival (e.g. after Min15Day backfill of an old window),
                    // delete the marker key in Redis: DEL bazaar_archive:{productId}:{bucketId}
                    consecutiveEmpty = 0;
                    consecutiveFailures = 0;
                    continue;
                }

                try
                {
                    var bucketStart = ArchivedMinuteBlock.BucketStart(bucketId);
                    var bucketEnd = ArchivedMinuteBlock.BucketEnd(bucketId);
                    var rows = await LoadLegacyRowsAsync(productId, bucketStart, bucketEnd).ConfigureAwait(false);
                    rowsArchived.Inc(rows.Count);

                    if (rows.Count == 0)
                    {
                        await db.StringSetAsync(markerKey, "empty").ConfigureAwait(false);
                        blocksEmpty.Inc();
                        consecutiveEmpty++;
                        consecutiveFailures = 0;
                        if (consecutiveEmpty >= emptyStreakLimit)
                        {
                            logger.LogInformation("Archive: {Product} hit empty streak {Streak} at bucket {Bucket}; stopping descent.", productId, consecutiveEmpty, bucketId);
                            return;
                        }
                        continue;
                    }

                    consecutiveEmpty = 0;
                    consecutiveFailures = 0;
                    var block = ArchivedMinuteBlock.FromRows(productId, bucketId, rows);
                    await blobStore.WriteBlobAsync(block, cancellationToken).ConfigureAwait(false);
                    await db.StringSetAsync(markerKey, "done").ConfigureAwait(false);
                    blocksArchived.Inc();

                    if (perBucketPauseMs > 0)
                        await Task.Delay(perBucketPauseMs, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    blocksFailed.Inc();
                    consecutiveFailures++;
                    logger.LogWarning(ex, "Archive: {Product} bucket {Bucket} failed (consecutive failures: {Failures})", productId, bucketId, consecutiveFailures);
                    // Do not mark done; will retry on next sweep.
                    if (consecutiveFailures >= failureCircuitBreakerThreshold)
                    {
                        logger.LogWarning("Archive: {Product} tripped circuit breaker after {Failures} consecutive failures; skipping the rest of this product for this sweep.", productId, consecutiveFailures);
                        return;
                    }
                    // Exponential backoff capped at 2 minutes: 5s, 10s, 20s, 40s, 80s.
                    var backoffSeconds = Math.Min(120, 5 * (int)Math.Pow(2, consecutiveFailures - 1));
                    await Task.Delay(TimeSpan.FromSeconds(backoffSeconds), cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private async Task<List<AggregatedQuickStatus>> LoadLegacyRowsAsync(string productId, DateTime bucketStart, DateTime bucketEnd)
        {
            var session = bazaar.Session;
            var table = BazaarService.GetLegacySplitMinutesTable(session);
            var quarters = QuartersOverlapping(bucketStart, bucketEnd);
            var collected = new List<AggregatedQuickStatus>();
            foreach (var q in quarters)
            {
                var rows = await table
                    .Where(r => r.ProductId == productId && r.QuaterId == q && r.TimeStamp >= bucketStart && r.TimeStamp < bucketEnd)
                    .ExecuteAsync().ConfigureAwait(false);
                collected.AddRange(rows);
            }
            // Sort ascending so deltas stay non-negative.
            collected.Sort((a, b) => a.TimeStamp.CompareTo(b.TimeStamp));
            return collected;
        }

        private static IEnumerable<short> QuartersOverlapping(DateTime start, DateTime end)
        {
            var first = SplitAggregatedQuickStatus.GetQuarterId(start);
            var last = SplitAggregatedQuickStatus.GetQuarterId(end.AddTicks(-1));
            for (var q = first; q <= last; q++)
                yield return q;
        }

        private static string MarkerKey(string productId, int bucketId) => "bazaar_archive:" + productId + ":" + bucketId;

        private const string LegacyDroppedMarker = "bazaar_archive:legacy_dropped";

        /// <summary>
        /// Drop the legacy <c>QuickStatusMin</c> table iff <c>HISTORY_ARCHIVE:DROP_LEGACY_TABLE=true</c>
        /// and it has not been dropped before. Runs once per cluster (idempotent via Redis marker).
        /// Operators are expected to manually verify that S3 archival is complete before flipping
        /// the flag; <see cref="BazaarService.LoadMinuteHistory"/> reads the same flag and skips
        /// the legacy fallback query when set so a dropped table does not cause read failures.
        /// </summary>
        private async Task MaybeDropLegacyTableAsync(CancellationToken cancellationToken)
        {
            if (!bool.TryParse(config["HISTORY_ARCHIVE:DROP_LEGACY_TABLE"], out var drop) || !drop)
                return;
            var db = redis.GetDatabase();
            if (await db.KeyExistsAsync(LegacyDroppedMarker).ConfigureAwait(false))
                return;
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
    }
}
