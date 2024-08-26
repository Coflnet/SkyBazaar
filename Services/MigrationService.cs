extern alias CoflCore;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using CoflCore::Coflnet.Cassandra;
using Coflnet.Sky.Kafka;
using Coflnet.Sky.SkyBazaar.Models;
using dev;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class MigrationService : BackgroundService
{
    private ISession session;
    private ISession oldSession;
    private ILogger<MigrationService> logger;
    private ConnectionMultiplexer redis;
    // get di
    private IServiceProvider serviceProvider;
    private IConfiguration config;
    public bool IsDone { get; private set; }

    public MigrationService(ISession session, OldSession oldSession, ILogger<MigrationService> logger, ConnectionMultiplexer redis, IServiceProvider serviceProvider, IConfiguration config)
    {
        this.session = session;
        this.logger = logger;
        this.redis = redis;
        this.serviceProvider = serviceProvider;
        this.oldSession = oldSession.Session;
        this.config = config;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var handlerLogger = serviceProvider.GetRequiredService<ILogger<MigrationHandler<AggregatedQuickStatus, SplitAggregatedQuickStatus>>>();
        var dailyLogger = serviceProvider.GetRequiredService<ILogger<MigrationHandler<AggregatedQuickStatus, AggregatedQuickStatus>>>();
        var dailyHandler = new MigrationHandler<AggregatedQuickStatus, AggregatedQuickStatus>(
                () => BazaarService.GetDaysTable(oldSession),
                session, dailyLogger, redis,
                () => BazaarService.GetDaysTable(session),
                a => a);
        await dailyHandler.Migrate();
        var hourlyHandler = new MigrationHandler<AggregatedQuickStatus, SplitAggregatedQuickStatus>(
                () => BazaarService.GetHoursTable(oldSession),
                session, handlerLogger, redis,
                () => BazaarService.GetSplitHoursTable(session),
                a => new SplitAggregatedQuickStatus(a));
        await hourlyHandler.Migrate();
        var minutehandler = new MigrationHandler<AggregatedQuickStatus, SplitAggregatedQuickStatus>(
                () => BazaarService.GetMinutesTable(oldSession),
                session, handlerLogger, redis,
                () => BazaarService.GetSplitMinutesTable(session),
                a => new SplitAggregatedQuickStatus(a));
        await minutehandler.Migrate();
        logger.LogInformation("Migrated, starting to replay kafka");
        using var scope = serviceProvider.CreateScope();
        var bazaarService = scope.ServiceProvider.GetRequiredService<BazaarService>();
        IsDone = true;
        await KafkaConsumer.ConsumeBatch<BazaarPull>(config, config["TOPICS:BAZAAR"], async bazaar =>
        {
            var session = await bazaarService.GetSession();
            Console.WriteLine($"retrieved batch {bazaar.Count()}, start processing in migration");
            try
            {
                await bazaarService.AddEntry(bazaar, session, true);
            }
            catch (Exception e)
            {
                logger.LogError(e, "saving");
                throw;
            }
            await bazaarService.CheckAggregation(session, bazaar);
        }, stoppingToken, "sky-bazaar-migrator", 5);
    }
}