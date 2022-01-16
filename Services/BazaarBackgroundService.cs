using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Coflnet.Sky.SkyAuctionTracker.Controllers;
using dev;
using System.Linq;
using System.Collections.Generic;

namespace Coflnet.Sky.SkyAuctionTracker.Services
{

    public class BazaarBackgroundService : BackgroundService
    {
        private IServiceScopeFactory scopeFactory;
        private IConfiguration config;
        private ILogger<BazaarBackgroundService> logger;

        Prometheus.Counter consumeCounter = Prometheus.Metrics.CreateCounter("sky_bazaar_consume_counter", "How many message batches were consumed from kafka");

        public BazaarBackgroundService(
            IServiceScopeFactory scopeFactory, IConfiguration config, ILogger<BazaarBackgroundService> logger)
        {
            this.scopeFactory = scopeFactory;
            this.config = config;
            this.logger = logger;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await GetService().Create();
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.WhenAny(GetConsumeTask(stoppingToken));
                logger.LogError("ended consumption");
            }
        }

        private Task GetConsumeTask(CancellationToken stoppingToken)
        {
            return Coflnet.Kafka.KafkaConsumer.ConsumeBatch<BazaarPull>(config["KAFKA_HOST"], config["TOPICS:BAZAAR"], async bazaar =>
            {
                consumeCounter.Inc();
                BazaarService service = GetService();
                var session = await service.GetSession();
                await Task.WhenAll(bazaar.Select(async (b) =>
                {
                    try
                    {
                        await service.AddEntry(b, session);
                    }
                    catch (System.Exception e)
                    {
                        logger.LogError(e, "saving");
                    }
                }));
            }, stoppingToken, "sky-bazaar-test", 50);
        }

        private BazaarService GetService()
        {
            return scopeFactory.CreateScope().ServiceProvider.GetRequiredService<BazaarService>();
        }
    }
}