using System.Threading.Tasks;
using System;
using System.Linq;
using Microsoft.Extensions.Configuration;
using dev;
using System.Collections.Generic;
using Cassandra;
using Cassandra.Data.Linq;
using Coflnet.Sky.SkyBazaar.Models;
using Cassandra.Mapping;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;
using Coflnet.Sky.Core;
using System.Linq.Expressions;
using RestSharp;
using System.Collections.Concurrent;
using System.Threading;

namespace Coflnet.Sky.SkyAuctionTracker.Services
{
    public interface ISessionContainer
    {
        /// <summary>
        /// The cassandra session
        /// </summary>
        ISession Session { get; }
    }
    public class BazaarService : ISessionContainer
    {
        private const string TABLE_NAME_DAILY_NEW = "QuickStatusDaly";
        private const string TABLE_NAME_DAILY = "QuickStatusDaly";
        private const string TABLE_NAME_HOURLY = "QuickStatusHourly";
        private const string TABLE_NAME_RECENT_HOURLY = "QuickStatusRecent3";
        private const string TABLE_NAME_MINUTES = "QuickStatusMin";
        private const string TABLE_NAME_SECONDS = "QuickStatusSeconds";
        private const string DEFAULT_ITEM_TAG = "STOCK_OF_STONKS";
        private static bool ranCreate;
        public DateTime LastSuccessfullDB { get; private set; }
        private IConfiguration config;
        private ILogger<BazaarService> logger;
        ISession _session;
        /// <summary>
        /// <inheritdoc/>
        /// </summary>
        public ISession Session => _session;

        private static Prometheus.Counter insertCount = Prometheus.Metrics.CreateCounter("sky_bazaar_status_insert", "How many inserts were made");
        private static Prometheus.Counter insertFailed = Prometheus.Metrics.CreateCounter("sky_bazaar_status_insert_failed", "How many inserts failed");
        private static Prometheus.Counter checkSuccess = Prometheus.Metrics.CreateCounter("sky_bazaar_check_success", "How elements where found in cassandra");
        private static Prometheus.Counter checkFail = Prometheus.Metrics.CreateCounter("sky_bazaar_check_fail", "How elements where not found in cassandra");
        private static Prometheus.Counter aggregateCount = Prometheus.Metrics.CreateCounter("sky_bazaar_aggregation", "How many aggregations were started (should be one every 5 min)");

        private List<StorageQuickStatus> currentState = new List<StorageQuickStatus>();
        private SemaphoreSlim sessionOpenLock = new SemaphoreSlim(1);
        private SemaphoreSlim insertConcurrencyLock = new SemaphoreSlim(20);

        private static readonly Prometheus.Histogram checkSuccessHistogram = Prometheus.Metrics
            .CreateHistogram("sky_bazaar_check_success_histogram", "Histogram of successfuly checked elements",
                new Prometheus.HistogramConfiguration
                {
                    Buckets = Prometheus.Histogram.LinearBuckets(start: 1, width: 10_000_000, count: 40)
                }
            );

        private static readonly Prometheus.Histogram checkFailHistogram = Prometheus.Metrics
            .CreateHistogram("sky_bazaar_check_fail_histogram", "Histogram of failed checked elements",
                new Prometheus.HistogramConfiguration
                {
                    Buckets = Prometheus.Histogram.LinearBuckets(start: 1, width: 10_000_000, count: 40)
                }
            );

        public BazaarService(IConfiguration config, ILogger<BazaarService> logger, ISession session)
        {
            this.config = config;
            this.logger = logger;
            _session = session;
        }

        internal async Task NewPull(int i, BazaarPull bazaar)
        {
            await AddEntry(bazaar);
        }


        private static void RemoveRedundandInformation(int i, BazaarPull pull, List<BazaarPull> lastMinPulls)
        {
            var lastPull = lastMinPulls.First();
            var lastPullDic = lastPull
                    .Products.ToDictionary(p => p.ProductId);

            var sellChange = 0;
            var buyChange = 0;
            var productCount = pull.Products.Count;

            var toRemove = new List<ProductInfo>();

            for (int index = 0; index < productCount; index++)
            {
                var currentProduct = pull.Products[index];
                var currentStatus = currentProduct.QuickStatus;
                var lastProduct = lastMinPulls.SelectMany(p => p.Products)
                                .Where(p => p.ProductId == currentStatus.ProductId)
                                .OrderByDescending(p => p.Id)
                                .FirstOrDefault();

                var lastStatus = new QuickStatus();
                if (lastProduct != null)
                {
                    lastStatus = lastProduct.QuickStatus;
                }
                // = lastPullDic[currentStatus.ProductId].QuickStatus;

                var takeFactor = i % 60 == 0 ? 30 : 3;

                if (currentStatus.BuyOrders == lastStatus.BuyOrders)
                {
                    // nothing changed
                    currentProduct.BuySummery = null;
                    buyChange++;
                }
                else
                {
                    currentProduct.BuySummery = currentProduct.BuySummery.Take(takeFactor).ToList();
                }
                if (currentStatus.SellOrders == lastStatus.SellOrders)
                {
                    // nothing changed
                    currentProduct.SellSummary = null;
                    sellChange++;
                }
                else
                {
                    currentProduct.SellSummary = currentProduct.SellSummary.Take(takeFactor).ToList();
                }
                if (currentProduct.BuySummery == null && currentProduct.SellSummary == null)
                {
                    toRemove.Add(currentProduct);
                }
            }
            //Console.WriteLine($"Not saving {toRemove.Count}");

            foreach (var item in toRemove)
            {
                pull.Products.Remove(item);
            }

            Console.WriteLine($"  BuyChange: {productCount - buyChange}  SellChange: {productCount - sellChange}");
            //context.Update(lastPull);
        }

        internal async Task<IEnumerable<ItemPrice>> GetCurrentPrices(List<string> tags)
        {
            return currentState.Select(s => new ItemPrice
            {
                ProductId = s.ProductId,
                BuyPrice = s.BuyPrice,
                SellPrice = s.SellPrice
            });
        }


        internal async Task CheckAggregation(ISession session, IEnumerable<BazaarPull> bazaar)
        {
            var timestamp = bazaar.Last().Timestamp;
            var boundary = TimeSpan.FromMinutes(5);
            if (bazaar.All(b => IsTimestampWithinGroup(b.Timestamp, boundary)))
                return; // nothing to do
            Console.WriteLine("aggregating minutes " + timestamp);
            _ = Task.Run(async () =>
            {
                try
                {
                    await RunAgreggation(session, timestamp);
                }
                catch (System.Exception e)
                {
                    logger.LogError(e, "aggregation");
                }
            });
        }

        private async Task RunAgreggation(ISession session, DateTime timestamp)
        {
            aggregateCount.Inc();
            var ids = await GetAllItemIds();
            foreach (var itemId in ids)
            {
                try
                {
                    await AggregateMinutes(session, timestamp - TimeSpan.FromMinutes(30), TimeSpan.FromMinutes(10), itemId, timestamp + TimeSpan.FromSeconds(1));
                }
                catch (Exception e)
                {
                    if (e.Message.Contains("Unrecognized name quaterid"))
                        await Task.Delay(10_000);
                    logger.LogError(e, "aggregation failed for minutes");
                }
            }
            logger.LogInformation("aggregated minutes with timestamp " + timestamp);
            if (IsTimestampWithinGroup(timestamp, TimeSpan.FromHours(2)))
                return;
            Console.WriteLine("aggregating hours");
            foreach (var itemId in ids)
            {
                try
                {
                    await AggregateHours(session, timestamp - TimeSpan.FromHours(6), itemId);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "aggregation failed for hours");
                }
            }

            if (IsTimestampWithinGroup(timestamp, TimeSpan.FromDays(1)))
                return;
            foreach (var itemId in ids)
            {
                await AggregateDays(session, DateTime.UtcNow - TimeSpan.FromDays(2), itemId);
            }
        }

        private static bool IsTimestampWithinGroup(DateTime timestamp, TimeSpan boundary)
        {
            return timestamp.Subtract(TimeSpan.FromSeconds(20)).RoundDown(boundary) == timestamp.RoundDown(boundary);
        }

        public async Task Aggregate(ISession session)
        {
            var minutes = GetMinutesTable(session);

            // minute loop
            var startDate = new DateTime(2025, 1, 17);
            var length = TimeSpan.FromHours(6);
            // stonks have always been on bazaar
            string[] ids = await GetAllItemIds();
            var list = new ConcurrentQueue<string>();
            foreach (var item in ids)
            {
                var itemId = item;
                var minTime = new DateTime(2025, 1, 24);
                var maxTime = new DateTime(2025, 1, 26);
                var count = await GetDaysTable(session).Where(r => r.TimeStamp > minTime && r.TimeStamp < maxTime && r.ProductId == itemId).Count().ExecuteAsync();
                if (count != 0)
                {
                    Console.WriteLine("Item already aggregated " + itemId);
                    continue;
                }
                list.Enqueue(item);
            }
            await Task.Delay(60000);
            var workers = new List<Task>();
            for (int i = 0; i < 1; i++)
            {
                var worker = Task.Run(async () =>
                {
                    while (list.TryDequeue(out string itemId))
                    {
                        try
                        {
                            await NewMethod(session, startDate, length, itemId);
                        }
                        catch (Exception e)
                        {
                            logger.LogError(e, "aggregation failed");
                        }
                    }
                    Console.WriteLine("Done aggregating, yay ");

                });
                workers.Add(worker);
            }

            await Task.WhenAll(workers);
        }

        private static async Task NewMethod(ISession session, DateTime startDate, TimeSpan length, string itemId)
        {
            Console.WriteLine("doing: " + itemId);
            await AggregateMinutes(session, startDate, length, itemId, DateTime.UtcNow);
            // hour loop
            await AggregateHours(session, startDate, itemId);
            // day loop
            await AggregateDays(session, startDate, itemId);

            await Task.Delay(10000);
        }

        private static async Task AggregateDays(ISession session, DateTime startDate, string itemId)
        {
            await AggregateDaysData(session, startDate, TimeSpan.FromDays(2), itemId, GetNewDaysTable(session), async (a, b, c, d) =>
            {
                var block = await CreateBlockAggregated(a, b, c, d, GetSplitHoursTable(a));
                return block;
            }, TimeSpan.FromDays(1));

        }

        private static async Task AggregateDaysData(ISession session, DateTime startDate, TimeSpan length, string itemId, Table<AggregatedQuickStatus> minTable,
            Func<ISession, string, DateTime, DateTime, Task<AggregatedQuickStatus>> Aggregator, TimeSpan detailedLength, int minCount = 29, DateTime stopTime = default)
        {
            if (stopTime == default)
                stopTime = DateTime.UtcNow;
            for (var start = startDate; start + length < stopTime; start += length)
            {
                var end = start + length;
                Expression<Func<AggregatedQuickStatus, bool>> currentSelect = DaySelectExpression(itemId, start - detailedLength, end);
                // check the bigger table for existing records
                await AggregateAfterCheck<AggregatedQuickStatus>(session, length, itemId, minTable, Aggregator, detailedLength, minCount, start, end, currentSelect);
            }
        }

        private static async Task AggregateHours(ISession session, DateTime startDate, string itemId)
        {
            await AggregateMinutesData(session, startDate, TimeSpan.FromHours(4), itemId, GetSplitHoursTable(session), async (a, b, c, d) =>
            {
                var block = await CreateBlockAggregated(a, b, c, d, GetSplitMinutesTable(a));
                try
                {
                    if(block == null)
                        return null;
                    var blockCopy = new SplitAggregatedQuickStatus(block);
                    // round timestamp to make the compaction collide if multiple are inserted
                    block.TimeStamp = block.TimeStamp.RoundDown(TimeSpan.FromHours(2));
                    await RecentHoursTable(session).Insert(block).ExecuteAsync();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Failed to insert recent hours " + e);
                }
                return block;
            }, TimeSpan.FromHours(2));
        }

        private static async Task AggregateMinutes(ISession session, DateTime startDate, TimeSpan length, string itemId, DateTime endDate)
        {
            await AggregateMinutesData(session, startDate, length, itemId, GetSplitMinutesTable(session), CreateBlock, TimeSpan.FromMinutes(5), 29, endDate);
        }

        private static async Task<string[]> GetAllItemIds()
        {
            var client = new RestClient("https://sky.coflnet.com");
            var stringRes = await client.ExecuteAsync(new RestRequest("/api/items/bazaar/tags"));
            var ids = JsonConvert.DeserializeObject<string[]>(stringRes.Content);
            return ids;
        }

        private static async Task AggregateMinutesData(ISession session, DateTime startDate, TimeSpan length, string itemId, Table<SplitAggregatedQuickStatus> minTable,
            Func<ISession, string, DateTime, DateTime, Task<SplitAggregatedQuickStatus>> Aggregator, TimeSpan detailedLength, int minCount = 29, DateTime stopTime = default)
        {
            if (stopTime == default)
                stopTime = DateTime.UtcNow;
            for (var start = startDate; start + length < stopTime; start += length)
            {
                var end = start + length;
                Expression<Func<SplitAggregatedQuickStatus, bool>> currentSelect = SelectExpression(itemId, start - detailedLength, end);
                // check the bigger table for existing records
                await AggregateAfterCheck(session, length, itemId, minTable, Aggregator, detailedLength, minCount, start, end, currentSelect);
            }
        }

        private static async Task AggregateAfterCheck<T>(ISession session, TimeSpan length, string itemId, Table<T> minTable,
            Func<ISession, string, DateTime, DateTime, Task<T>> Aggregator, TimeSpan detailedLength, int minCount, DateTime start, DateTime end, Expression<Func<T, bool>> currentSelect)
             where T : AggregatedQuickStatus
        {
            var existing = await minTable.Where(currentSelect).ExecuteAsync();
            var lookup = existing.GroupBy(e => e.TimeStamp.RoundDown(detailedLength)).Select(e => e.First()).ToDictionary(e => e.TimeStamp.RoundDown(detailedLength));
            var addCount = 0;
            var skipped = 0;
            var lineMinCount = start < new DateTime(2022, 1, 1) ? 1 : minCount;
            for (var detailedStart = start; detailedStart < end; detailedStart += detailedLength)
            {
                if (lookup.TryGetValue(detailedStart.RoundDown(detailedLength), out T sum) && sum.Count >= lineMinCount)
                {
                    skipped++;
                    continue;
                }

                var detailedEnd = detailedStart + detailedLength;
                T result = await Aggregator(session, itemId, detailedStart, detailedEnd);
                if (result == null)
                    continue;
                await session.ExecuteAsync(minTable.Insert(result));
                addCount += result.Count;
            }
            if (length < TimeSpan.FromMinutes(10) && Random.Shared.NextDouble() > 0.1)
                return;
            Console.WriteLine($"checked {start} {minTable.Name} {itemId} {addCount}\t{skipped}");
        }

        private static async Task<SplitAggregatedQuickStatus> CreateBlock(ISession session, string itemId, DateTime detailedStart, DateTime detailedEnd)
        {
            var block = (await GetSmalestTable(session).Where(a => a.ProductId == itemId && a.TimeStamp >= detailedStart && a.TimeStamp < detailedEnd).ExecuteAsync())
                        .ToList().Select(qs =>
                        {
                            qs.BuyPrice = qs.BuyOrders.FirstOrDefault()?.PricePerUnit ?? qs.BuyPrice;
                            qs.SellPrice = qs.SellOrders.FirstOrDefault()?.PricePerUnit ?? qs.SellPrice;
                            return qs;
                        });
            if (block.Count() == 0)
                return null; // no data for this 
            var result = new AggregatedQuickStatus(block.First());
            result.MaxBuy = (float)block.Max(b => b.BuyPrice);
            result.MaxSell = (float)block.Max(b => b.SellPrice);
            result.MinBuy = (float)block.Min(b => b.BuyPrice);
            result.MinSell = (float)block.Min(b => b.SellPrice);
            result.Count = (short)block.Count();
            return new(result);
        }
        private static async Task<SplitAggregatedQuickStatus> CreateBlockAggregated(ISession session, string itemId, DateTime detailedStart, DateTime detailedEnd, Table<SplitAggregatedQuickStatus> startingTable)
        {
            var quarter = SplitAggregatedQuickStatus.GetQuarterId(detailedEnd - TimeSpan.FromSeconds(1));
            var block = (await startingTable.Where(a => a.ProductId == itemId && a.TimeStamp >= detailedStart && a.TimeStamp < detailedEnd && a.QuaterId == quarter).ExecuteAsync()).ToList();
            if (block.Count() == 0)
                return null; // no data for this 
            var result = new AggregatedQuickStatus(block.First())
            {
                MaxBuy = (float)block.Max(b => b.MaxBuy),
                MaxSell = (float)block.Max(b => b.MaxSell),
                MinBuy = (float)block.Min(b => b.MinBuy),
                MinSell = (float)block.Min(b => b.MinSell),
                Count = (short)block.Sum(b => b.Count)
            };
            return new(result);
        }

        private static Expression<Func<SplitAggregatedQuickStatus, bool>> SelectExpression(string itemId, DateTime start, DateTime end)
        {
            var quarter = SplitAggregatedQuickStatus.GetQuarterId(end);
            return a => a.ProductId == itemId && a.TimeStamp >= start && a.TimeStamp < end && quarter == a.QuaterId;
        }
        private static Expression<Func<AggregatedQuickStatus, bool>> DaySelectExpression(string itemId, DateTime start, DateTime end)
        {
            return a => a.ProductId == itemId && a.TimeStamp >= start && a.TimeStamp < end;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public async Task Create(ISession session = null)
        {
            if (ranCreate)
                return;
            ranCreate = true;

            session ??= await GetSession();

            // await session.ExecuteAsync(new SimpleStatement("DROP table Flip;"));
            Table<StorageQuickStatus> tenseconds = GetSplitSmalestTable(session);
            await tenseconds.CreateIfNotExistsAsync();

            var minutes = GetSplitMinutesTable(session);
            await minutes.CreateIfNotExistsAsync();
            var hours = GetSplitHoursTable(session);
            await hours.CreateIfNotExistsAsync();
            var daily = GetNewDaysTable(session);
            await daily.CreateIfNotExistsAsync();
            var recentHours = RecentHoursTable(session);
            await recentHours.CreateIfNotExistsAsync();

            try
            {
                // Query current compaction strategy for recent hours table
                var compactionResult = await session.ExecuteAsync(new SimpleStatement($"SELECT compaction, default_time_to_live FROM system_schema.tables WHERE keyspace_name = '{session.Keyspace}' AND table_name = '{TABLE_NAME_RECENT_HOURLY.ToLower()}';"));
                var row = compactionResult.FirstOrDefault();
                var keeptime = 1209600 / 2;
                bool needsUpdate = true;
                if (row != null)
                {
                    var compaction = row.GetValue<IDictionary<string, string>>("compaction");
                    var ttl = row.GetValue<int?>("default_time_to_live");
                    if (compaction != null && compaction.TryGetValue("class", out var compactionClass))
                    {
                        if (compactionClass.Contains("TimeWindowCompactionStrategy") && ttl == keeptime)
                        {
                            needsUpdate = false;
                        }
                    }
                }
                if (needsUpdate)
                {
                    await session.ExecuteAsync(new SimpleStatement(
                        $"ALTER TABLE {TABLE_NAME_RECENT_HOURLY} WITH compaction = {{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1', 'compaction_window_unit': 'DAYS'}} AND default_time_to_live = {keeptime};"));
                    await session.ExecuteAsync(new SimpleStatement(
                        $"ALTER TABLE {TABLE_NAME_SECONDS} WITH compaction = {{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1', 'compaction_window_unit': 'DAYS'}} AND default_time_to_live = 1209600;"));
                    logger.LogInformation("Set compaction strategy for recent hours table");
                }
                else
                {
                    logger.LogInformation("Recent hours table already has the correct compaction strategy and TTL.");
                }
            }
            catch (Exception e)
            {
                logger.LogError(e, "compaction strategy");
            }
            //Console.WriteLine("there are this many booster cookies: " + JsonConvert.SerializeObject(session.Execute("Select count(*) from " + TABLE_NAME_SECONDS + " where ProductId = 'BOOSTER_COOKIE' and Timestamp > '2021-12-07'").FirstOrDefault()));
        }

        public static Table<AggregatedQuickStatus> GetDaysTable(ISession session)
        {
            return new Table<AggregatedQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_DAILY);
        }
        public static Table<AggregatedQuickStatus> GetNewDaysTable(ISession session)
        {
            return new Table<AggregatedQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_DAILY_NEW);
        }

        public static Table<AggregatedQuickStatus> GetHoursTable(ISession session)
        {
            return new Table<AggregatedQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_HOURLY);
        }

        public static Table<AggregatedQuickStatus> GetMinutesTable(ISession session)
        {
            return new Table<AggregatedQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_MINUTES);
        }

        public static Table<StorageQuickStatus> GetSmalestTable(ISession session)
        {
            return new Table<StorageQuickStatus>(session, new MappingConfiguration(), TABLE_NAME_SECONDS);
        }
        public static Table<SplitAggregatedQuickStatus> GetSplitHoursTable(ISession session)
        {
            var mapping = new MappingConfiguration().Define(
                new Map<SplitAggregatedQuickStatus>()
                    .PartitionKey(f => f.ProductId, f => f.QuaterId)
                    .ClusteringKey(f => f.TimeStamp)
                    .Column(f => f.BuyOrders, cm => cm.Ignore())
                    .Column(f => f.SellOrders, cm => cm.Ignore())
                    .TableName(TABLE_NAME_HOURLY)
            );
            return new Table<SplitAggregatedQuickStatus>(session, mapping, TABLE_NAME_HOURLY);
        }


        public static Table<AggregatedQuickStatus> RecentHoursTable(ISession session)
        {
            var mapping = new MappingConfiguration().Define(
                new Map<AggregatedQuickStatus>()
                    .PartitionKey(f => f.TimeStamp)
                    .ClusteringKey(f => f.ProductId)
                    .Column(f => f.BuyOrders, cm => cm.Ignore())
                    .Column(f => f.SellOrders, cm => cm.Ignore())
                    .TableName(TABLE_NAME_RECENT_HOURLY)
            );
            return new Table<AggregatedQuickStatus>(session, mapping, TABLE_NAME_RECENT_HOURLY);
        }
        public static Table<SplitAggregatedQuickStatus> GetSplitMinutesTable(ISession session)
        {
            var mapping = new MappingConfiguration().Define(
                new Map<SplitAggregatedQuickStatus>()
                    .PartitionKey(f => f.ProductId, f => f.QuaterId)
                    .ClusteringKey(f => f.TimeStamp)
                    .Column(f => f.BuyOrders, cm => cm.Ignore())
                    .Column(f => f.SellOrders, cm => cm.Ignore())
                    .TableName(TABLE_NAME_MINUTES)
            );
            return new Table<SplitAggregatedQuickStatus>(session, mapping, TABLE_NAME_MINUTES);
        }

        public static Table<StorageQuickStatus> GetSplitSmalestTable(ISession session)
        {
            var mapping = new MappingConfiguration().Define(
                new Map<SplitStorageQuickStatus>()
                    .PartitionKey(f => f.ProductId, f => f.WeekId)
                    .ClusteringKey(f => f.TimeStamp)
                    .Column(f => f.BuyOrders, cm => cm.Ignore())
                    .Column(f => f.SellOrders, cm => cm.Ignore())
                    .TableName(TABLE_NAME_SECONDS)
            );
            return new Table<StorageQuickStatus>(session, mapping, TABLE_NAME_SECONDS);
        }

        public async Task AddEntry(BazaarPull pull)
        {
            var session = await GetSession();
            await AddEntry(new List<BazaarPull> { pull }, session);
        }

        public async Task AddEntry(IEnumerable<BazaarPull> pull, ISession session = null, bool useSplit = false)
        {
            if (session == null)
                session = await GetSession();

            //session.CreateKeyspaceIfNotExists("bazaar_quickstatus");
            //session.ChangeKeyspace("bazaar_quickstatus");
            //var mapper = new Mapper(session);
            var table = GetSmalestTable(session);
            var inserts = pull.SelectMany(p => p.Products.Select(item =>
            {
                if (item.QuickStatus == null)
                    throw new NullReferenceException("Quickstatus can't be null " + item.ProductId);
                var flip = new StorageQuickStatus()
                {
                    TimeStamp = p.Timestamp,
                    ProductId = item.ProductId,
                    SerialisedBuyOrders = MessagePack.MessagePackSerializer.Serialize(item.BuySummery),
                    SerialisedSellOrders = MessagePack.MessagePackSerializer.Serialize(item.SellSummary),
                    BuyMovingWeek = item.QuickStatus.BuyMovingWeek,
                    BuyOrdersCount = item.QuickStatus.BuyOrders,
                    BuyPrice = item.QuickStatus.BuyPrice,
                    BuyVolume = item.QuickStatus.BuyVolume,
                    SellMovingWeek = item.QuickStatus.SellMovingWeek,
                    SellOrdersCount = item.QuickStatus.SellOrders,
                    SellPrice = item.QuickStatus.SellPrice,
                    SellVolume = item.QuickStatus.SellVolume,
                    ReferenceId = item.Id
                };
                return flip;
            })).ToList();

            currentState = inserts;
            var splitTable = GetSplitSmalestTable(session);

            Console.WriteLine($"inserting {string.Join(',', pull.Select(p => p.Timestamp))}   at {DateTime.UtcNow}");
            await Task.WhenAll(inserts.GroupBy(i => i.ProductId).Select(async status =>
            {
                var maxTries = 7;
                var statement = new BatchStatement();
                foreach (var item in status)
                {
                    statement.Add(table.Insert(item));
                }
                for (int i = 0; i < maxTries; i++)
                    try
                    {
                        await insertConcurrencyLock.WaitAsync();
                        statement.SetConsistencyLevel(ConsistencyLevel.Quorum);
                        statement.SetRoutingKey(splitTable.Insert(status.First()).RoutingKey);
                        await session.ExecuteAsync((IStatement)statement);
                        insertCount.Inc();
                        LastSuccessfullDB = DateTime.UtcNow;
                        return;
                    }
                    catch (Exception e)
                    {
                        insertFailed.Inc();
                        logger.LogError(e, $"storing {status.Key} {status.First().TimeStamp} failed {i} times");
                        await Task.Delay(500 * (i + 1));
                        if (i >= maxTries - 1)
                            throw;
                    }
                    finally
                    {
                        insertConcurrencyLock.Release();
                    }
            }));
            return;
        }

        public async Task<ISession> GetSession()
        {
            if (_session != null)
                return _session;
            throw new NotImplementedException("the session should be injected from DI");
        }

        public async Task<IEnumerable<AggregatedQuickStatus>> GetStatus(string productId, DateTime start, DateTime end, int count = 1)
        {
            if (end == default)
                end = DateTime.UtcNow;
            if (start == default)
                start = new DateTime(2020, 3, 10);
            var session = await GetSession();
            var mapper = new Mapper(session);
            string tableName = GetTable(start, end);
            //return await GetSmalestTable(session).Where(f => f.ProductId == productId && f.TimeStamp <= end && f.TimeStamp > start).Take(count).ExecuteAsync();
            if (tableName == TABLE_NAME_SECONDS)
            {
                var result = (await GetSplitSmalestTable(session).Where(f => f.ProductId == productId && f.TimeStamp <= end && f.TimeStamp > start)
                    .OrderByDescending(d => d.TimeStamp).Take(count).ExecuteAsync().ConfigureAwait(false))
                    .ToList().Select(s => new AggregatedQuickStatus(s));
                if (result.Count() == 0 && start > DateTime.UtcNow - TimeSpan.FromMinutes(5))
                {
                    return currentState.Where(c => c.ProductId == productId).Select(c => new AggregatedQuickStatus(c));
                }
                LastSuccessfullDB = DateTime.UtcNow;
                return result;
            }
            if (tableName == TABLE_NAME_DAILY_NEW)
                return await mapper.FetchAsync<AggregatedQuickStatus>("SELECT * FROM " + tableName
                    + " where ProductId = ? and TimeStamp > ? and TimeStamp <= ? Order by Timestamp DESC", productId, start, end).ConfigureAwait(false);
            var quarterId = SplitAggregatedQuickStatus.GetQuarterId(end);
            var loadedFlip = await mapper.FetchAsync<SplitAggregatedQuickStatus>("SELECT * FROM " + tableName
                    + " where ProductId = ? and TimeStamp > ? and TimeStamp <= ? and QuaterId = ? Order by Timestamp DESC", productId, start, end, quarterId).ConfigureAwait(false);
            return loadedFlip.ToList();
        }

        private static string GetTable(DateTime start, DateTime end)
        {
            var length = (end - start);
            if (length < TimeSpan.FromHours(1))
                return TABLE_NAME_SECONDS;  // one every 10/20 seconds
            if (length < TimeSpan.FromHours(24))
                return TABLE_NAME_MINUTES; // 1 per 5 min
            if (length < TimeSpan.FromDays(7.01f))
                return TABLE_NAME_HOURLY; // 1 per 2 hours
            return TABLE_NAME_DAILY; // one daily
        }

        internal async Task<IEnumerable<ItemPriceMovement>> GetMovement(int hours, bool usebuyOrders)
        {
            var table = RecentHoursTable(await GetSession());
            var start = (DateTime.UtcNow - TimeSpan.FromHours(hours)).RoundDown(TimeSpan.FromHours(2)); // hourly view is saved every 2 hours
            var prices = (await table.Where(t => t.TimeStamp == start).ExecuteAsync()).ToList();
            if (prices.Count() == 0)
            {
                // try with more recent data
                start = (DateTime.UtcNow - TimeSpan.FromHours(hours - 2)).RoundDown(TimeSpan.FromHours(2));
                prices = (await table.Where(t => t.TimeStamp == start).ExecuteAsync()).ToList();
            }
            var currentLookup = currentState.GroupBy(c=>c.ProductId).Select(g=>g.First()).ToDictionary(c => c.ProductId, c => c);
            return prices
                .GroupBy(p => p.ProductId).Select(g => g.FirstOrDefault())
                .Select(p =>
            {
                var item = new ItemPriceMovement
                {
                    ItemId = p.ProductId,
                    PreviousPrice = p.BuyPrice,
                    CurrentPrice = currentLookup[p.ProductId].BuyPrice,
                    Volume = currentLookup[p.ProductId].BuyVolume,
                };
                if (usebuyOrders)
                {
                    item.PreviousPrice = p.SellPrice;
                    item.CurrentPrice = currentLookup[p.ProductId].SellPrice;
                    item.Volume = currentLookup[p.ProductId].SellVolume;
                }
                return item;
            });
        }
    }
}
