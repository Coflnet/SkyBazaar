using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Coflnet.Sky.SkyBazaar.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

/// <summary>Settings of one rebuild run (all read from <c>HISTORY_REBUILD:*</c>).</summary>
internal sealed record RebuildOptions(bool DryRun, DateTime Start, DateTime End, int DelayMs);

/// <summary>
/// One-off job (off by default) that recomputes the daily rows of <c>QuickStatusDaly</c> from the 5-minute rows of every tier
/// (hot 15-day table, R2 archive blob, legacy table) with the zero-aware <see cref="HistoryAggregation"/>.
/// Per product it walks the 30-day archive buckets oldest to newest, groups the merged 5-minute rows by UTC day and, for days with
/// at least <see cref="MinRowsPerDay"/> rows, inserts the canonical row (TimeStamp = day 00:00 UTC) and then deletes the old rows of
/// that day with another timestamp (primary key is (ProductId, TimeStamp)), so a day is never without a row.
/// Progress is kept in Redis <c>bazaar_daily_rebuild:v1:{product}</c> (last completed bucket id); a dry run writes nothing, not even Redis.
/// </summary>
public class DailyHistoryRebuildService : BackgroundService
{
    internal const int MinRowsPerDay = 12;
    internal const string MarkerPrefix = "bazaar_daily_rebuild:v1:";

    private readonly BazaarService bazaar;
    private readonly IBlobHistoryStore blobStore;
    private readonly IConnectionMultiplexer redis;
    private readonly IConfiguration config;
    private readonly ILogger<DailyHistoryRebuildService> logger;

    private static readonly Prometheus.Counter daysWritten = Prometheus.Metrics.CreateCounter("sky_bazaar_daily_rebuild_days_written", "Daily rows written (or that would be written in a dry run) by the daily rebuild");
    private static readonly Prometheus.Counter daysSkipped = Prometheus.Metrics.CreateCounter("sky_bazaar_daily_rebuild_days_skipped", "Days skipped by the daily rebuild because they had too few 5-minute rows");
    private static readonly Prometheus.Counter rowsDeleted = Prometheus.Metrics.CreateCounter("sky_bazaar_daily_rebuild_rows_deleted", "Old daily rows deleted (or that would be deleted) by the daily rebuild");
    private static readonly Prometheus.Counter failures = Prometheus.Metrics.CreateCounter("sky_bazaar_daily_rebuild_failures", "Product buckets the daily rebuild failed on");
    private static readonly Prometheus.Counter productsDone = Prometheus.Metrics.CreateCounter("sky_bazaar_daily_rebuild_products_done", "Products the daily rebuild finished without failure");

    /// <summary>DI constructor.</summary>
    public DailyHistoryRebuildService(
        BazaarService bazaar,
        IBlobHistoryStore blobStore,
        IConnectionMultiplexer redis,
        IConfiguration config,
        ILogger<DailyHistoryRebuildService> logger)
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
        if (!bool.TryParse(config["HISTORY_REBUILD:ENABLED"], out var enabled) || !enabled)
            return;
        try
        {
            var options = ReadOptions();
            if (!CanRun(options))
            {
                logger.LogError("HISTORY_REBUILD with DRY_RUN=false needs the R2 cold tier (blob store is not enabled); refusing to start so no bucket is rebuilt from partial data.");
                return;
            }
            var products = await ResolveProductsAsync(stoppingToken).ConfigureAwait(false);
            logger.LogInformation("Daily rebuild starting: {Count} products, {Start:O} to {End:O}, dryRun={Dry}", products.Count, options.Start, options.End, options.DryRun);
            await RunAsync(products, options, redis.GetDatabase(), stoppingToken).ConfigureAwait(false);
            logger.LogInformation("Daily rebuild finished (dryRun={Dry})", options.DryRun);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // shutting down
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Daily rebuild aborted");
        }
    }

    /// <summary>A real (non dry) run needs the R2 tier, otherwise days whose data lives only there would be judged on partial rows.</summary>
    internal bool CanRun(RebuildOptions options) => options.DryRun || (blobStore != null && blobStore.IsEnabled);

    /// <summary>True when the exception is a real shutdown; an HTTP/R2 timeout (also an OperationCanceledException) is not.</summary>
    private static bool IsShutdown(Exception ex, CancellationToken ct) => ex is OperationCanceledException && ct.IsCancellationRequested;

    internal RebuildOptions ReadOptions()
    {
        var dryRun = !bool.TryParse(config["HISTORY_REBUILD:DRY_RUN"], out var dry) || dry;
        var start = ParseDate(config["HISTORY_REBUILD:START"], new DateTime(2021, 7, 1, 0, 0, 0, DateTimeKind.Utc));
        var end = ParseDate(config["HISTORY_REBUILD:END"], DateTime.UtcNow.Date.AddDays(-2));
        var delay = int.TryParse(config["HISTORY_REBUILD:DELAY_MS"], out var d) ? d : 500;
        return new RebuildOptions(dryRun, start, end, delay);
    }

    private static DateTime ParseDate(string value, DateTime fallback)
    {
        if (!DateTime.TryParse(value, System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal, out var parsed))
            return fallback;
        return parsed;
    }

    private async Task<IReadOnlyList<string>> ResolveProductsAsync(CancellationToken ct)
    {
        var configured = config["HISTORY_REBUILD:PRODUCTS"];
        if (!string.IsNullOrWhiteSpace(configured))
            return configured.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        for (var attempt = 0; attempt < 20; attempt++)
        {
            var known = GetProducts();
            if (known.Count > 0)
                return known;
            await Task.Delay(TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
        }
        return Array.Empty<string>();
    }

    /// <summary>Products of the live state, the same source the archive sweep uses.</summary>
    protected virtual IReadOnlyList<string> GetProducts() => bazaar.GetKnownProductIds();

    /// <summary>Rebuilds all products one after another; a failing product never stops the others.</summary>
    internal async Task RunAsync(IReadOnlyList<string> products, RebuildOptions options, IDatabase db, CancellationToken ct)
    {
        foreach (var product in products)
        {
            ct.ThrowIfCancellationRequested();
            await ProcessProductSafeAsync(product, options, db, ct).ConfigureAwait(false);
            if (options.DelayMs > 0)
                await Task.Delay(options.DelayMs, ct).ConfigureAwait(false);
        }
    }

    private async Task ProcessProductSafeAsync(string product, RebuildOptions options, IDatabase db, CancellationToken ct)
    {
        try
        {
            await ProcessProductAsync(product, options, db, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (!IsShutdown(ex, ct))
        {
            failures.Inc();
            logger.LogError(ex, "Daily rebuild failed for {Product}; continuing with the next product", product);
        }
    }

    /// <summary>Walks the buckets of one product oldest to newest, resuming after the Redis marker.</summary>
    internal async Task<RebuildSummary> ProcessProductAsync(string product, RebuildOptions options, IDatabase db, CancellationToken ct)
    {
        var summary = new RebuildSummary();
        var firstBucket = ArchivedMinuteBlock.GetBucketId(options.Start);
        var lastBucket = ArchivedMinuteBlock.GetBucketId(options.End);
        var done = await ReadMarkerAsync(product, db).ConfigureAwait(false);
        for (var bucket = Math.Max(firstBucket, done + 1); bucket <= lastBucket; bucket++)
        {
            ct.ThrowIfCancellationRequested();
            int rowCount;
            try
            {
                rowCount = await ProcessBucketAsync(product, bucket, options, summary, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (!IsShutdown(ex, ct))
            {
                failures.Inc();
                logger.LogError(ex, "Daily rebuild failed for {Product} bucket {Bucket}; marker stays at {Done}", product, bucket, done);
                LogSummary(product, options, summary, failed: true);
                return summary;
            }
            // a bucket without any row (e.g. a tier was unreadable or the product did not exist yet) is not marked done
            if (!options.DryRun && rowCount > 0 && ArchivedMinuteBlock.BucketEnd(bucket) <= options.End.AddDays(1))
                await db.StringSetAsync(MarkerPrefix + product, bucket).ConfigureAwait(false);
            done = bucket;
        }
        productsDone.Inc();
        LogSummary(product, options, summary, failed: false);
        return summary;
    }

    private static async Task<int> ReadMarkerAsync(string product, IDatabase db)
    {
        var value = await db.StringGetAsync(MarkerPrefix + product).ConfigureAwait(false);
        return value.HasValue && int.TryParse((string)value, out var bucket) ? bucket : int.MinValue / 2;
    }

    private void LogSummary(string product, RebuildOptions options, RebuildSummary s, bool failed)
    {
        logger.LogInformation("Daily rebuild {Product}: dryRun={Dry} failed={Failed} written={Written} skipped={Skipped} deleted={Deleted}",
            product, options.DryRun, failed, s.Written, s.Skipped, s.Deleted);
    }

    internal sealed class RebuildSummary
    {
        public int Written;
        public int Skipped;
        public int Deleted;
    }

    private async Task<int> ProcessBucketAsync(string product, int bucket, RebuildOptions options, RebuildSummary summary, CancellationToken ct)
    {
        var rows = await LoadBucketRowsAsync(product, bucket, ct).ConfigureAwait(false);
        foreach (var day in GroupByUtcDay(rows).OrderBy(g => g.Key))
        {
            ct.ThrowIfCancellationRequested();
            if (day.Key < options.Start.Date || day.Key > options.End.Date)
                continue;
            await ProcessDayAsync(product, day.Key, day.ToList(), options, summary).ConfigureAwait(false);
        }
        return rows.Count;
    }

    /// <summary>Groups rows by their UTC calendar day (key = day 00:00 UTC).</summary>
    internal static IEnumerable<IGrouping<DateTime, AggregatedQuickStatus>> GroupByUtcDay(IEnumerable<AggregatedQuickStatus> rows)
        => rows.GroupBy(r => new DateTime(r.TimeStamp.Ticks / TimeSpan.TicksPerDay * TimeSpan.TicksPerDay, DateTimeKind.Utc));

    private async Task ProcessDayAsync(string product, DateTime day, List<AggregatedQuickStatus> rows, RebuildOptions options, RebuildSummary summary)
    {
        if (rows.Count < MinRowsPerDay)
        {
            summary.Skipped++;
            daysSkipped.Inc();
            return;
        }
        rows.Sort((a, b) => a.TimeStamp.CompareTo(b.TimeStamp));
        var canonical = HistoryAggregation.Aggregate(rows, day);
        canonical.ProductId = product;
        summary.Written++;
        daysWritten.Inc();
        await WriteDayAsync(product, day, canonical, summary, options.DryRun).ConfigureAwait(false);
    }

    /// <summary>
    /// Inserts the canonical row first, then deletes every other row of the day by exact primary key.
    /// A dry run only reads the existing rows and counts what would be deleted.
    /// </summary>
    private async Task WriteDayAsync(string product, DateTime day, AggregatedQuickStatus canonical, RebuildSummary summary, bool dryRun)
    {
        var existing = await ReadDailyRowsAsync(product, day, day.AddDays(1)).ConfigureAwait(false);
        if (!dryRun)
            await InsertDailyAsync(canonical).ConfigureAwait(false);
        foreach (var old in existing.Where(e => e.TimeStamp.Ticks != day.Ticks))
        {
            if (!dryRun)
                await DeleteDailyAsync(product, old.TimeStamp).ConfigureAwait(false);
            summary.Deleted++;
            rowsDeleted.Inc();
        }
    }

    /// <summary>All 5-minute rows of one 30-day bucket from every tier, merged (higher count, then rows with order books win).</summary>
    protected virtual async Task<List<AggregatedQuickStatus>> LoadBucketRowsAsync(string product, int bucket, CancellationToken ct)
    {
        var start = ArchivedMinuteBlock.BucketStart(bucket);
        var end = ArchivedMinuteBlock.BucketEnd(bucket);
        var hot = await LoadHotRowsAsync(product, bucket, start, end).ConfigureAwait(false);
        var cold = await LoadColdRowsAsync(product, bucket, ct).ConfigureAwait(false);
        var legacy = await LoadLegacyRowsAsync(product, start, end).ConfigureAwait(false);
        return HistoryArchiveService.MergeForArchive(hot.Concat(cold), legacy);
    }

    /// <summary>Hot 15-day table rows (both partitions of the bucket).</summary>
    protected virtual Task<List<AggregatedQuickStatus>> LoadHotRowsAsync(string product, int bucket, DateTime start, DateTime end)
        => bazaar.LoadHotMinuteRowsAsync(product, bucket, start, end);

    /// <summary>Legacy table rows; empty once the legacy table is dropped.</summary>
    protected virtual Task<List<AggregatedQuickStatus>> LoadLegacyRowsAsync(string product, DateTime start, DateTime end)
        => bazaar.LoadLegacyMinuteRowsAsync(product, start, end);

    /// <summary>R2 archive blob rows, read uncached so the sliding cache does not fill memory.</summary>
    protected virtual async Task<List<AggregatedQuickStatus>> LoadColdRowsAsync(string product, int bucket, CancellationToken ct)
    {
        if (blobStore == null || !blobStore.IsEnabled)
            return new List<AggregatedQuickStatus>();
        var block = await blobStore.ReadBlobUncachedAsync(product, bucket, ct).ConfigureAwait(false);
        return block?.ToRows().ToList() ?? new List<AggregatedQuickStatus>();
    }

    /// <summary>Existing daily rows in [start, end).</summary>
    protected virtual async Task<List<AggregatedQuickStatus>> ReadDailyRowsAsync(string product, DateTime start, DateTime end)
    {
        var rows = await BazaarService.GetNewDaysTable(bazaar.Session)
            .Where(r => r.ProductId == product && r.TimeStamp >= start && r.TimeStamp < end)
            .ExecuteAsync().ConfigureAwait(false);
        return rows.ToList();
    }

    /// <summary>Upserts one daily row.</summary>
    protected virtual Task InsertDailyAsync(AggregatedQuickStatus row)
        => BazaarService.GetNewDaysTable(bazaar.Session).Insert(row).ExecuteAsync();

    /// <summary>Deletes one daily row by its exact primary key (ProductId, TimeStamp).</summary>
    protected virtual Task DeleteDailyAsync(string product, DateTime timeStamp)
        => BazaarService.GetNewDaysTable(bazaar.Session)
            .Where(r => r.ProductId == product && r.TimeStamp == timeStamp)
            .Delete().ExecuteAsync();
}
