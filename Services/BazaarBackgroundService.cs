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
using System;
using Cassandra;

namespace Coflnet.Sky.SkyAuctionTracker.Services
{

    public class BazaarBackgroundService : BackgroundService
    {
        private BazaarService bazaarService;
        private OrderBookService orderBookService;
        private IConfiguration config;
        private ILogger<BazaarBackgroundService> logger;

        Prometheus.Counter consumeCounter = Prometheus.Metrics.CreateCounter("sky_bazaar_consume_counter", "How many message batches were consumed from kafka");

        public BazaarBackgroundService(
            IConfiguration config, ILogger<BazaarBackgroundService> logger, OrderBookService orderBookService, BazaarService bazaarService)
        {
            this.config = config;
            this.logger = logger;
            this.orderBookService = orderBookService;
            this.bazaarService = bazaarService;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // Restore matching independently of history-table setup and HTTP startup.
            // Until ready, personal observations retry and fast price observations are dropped.
            _ = orderBookService.Load().ContinueWith(t =>
            {
                if (t.IsFaulted)
                    logger.LogError(t.Exception, "order book background load failed");
            }, TaskScheduler.Default);
            await bazaarService.Create();
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await GetConsumeTask(stoppingToken);
                }
                catch (NoHostAvailableException e)
                {
                    logger.LogError(e, "");
                    logger.LogError(Newtonsoft.Json.JsonConvert.SerializeObject(e.Errors));
                }
                catch (Exception e)
                {
                    logger.LogError(e, "consume & insert");
                }
                logger.LogError("ended consumption");

            }
        }

        private Task GetConsumeTask(CancellationToken stoppingToken)
        {
            logger.LogInformation($"starting consumption from {config["TOPICS:BAZAAR"]}");
            return Kafka.KafkaConsumer.ConsumeBatch<BazaarPull>(config, config["TOPICS:BAZAAR"], async bazaar =>
            {
                consumeCounter.Inc();
                foreach (var b in bazaar)
                {
                    if (b.Timestamp < DateTime.Now - TimeSpan.FromHours(2))
                    {
                        BazaarTelemetry.Observation(logger, "kafka", null, b.Timestamp, "expired");
                        continue;
                    }
                    try
                    {
                        await orderBookService.BazaarPull(b);
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e, "orderbook check");
                    }
                }
                // Matching must not wait for historical price storage.
                var session = await bazaarService.GetSession();
                Console.WriteLine($"retrieved batch {bazaar.Count()}, start processing");
                try
                {
                    await bazaarService.AddEntry(bazaar, session);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "saving bazaar batch");
                    throw;
                }
                await bazaarService.CheckAggregation(session, bazaar);
            }, stoppingToken, config["KAFKA:CONSUMER_GROUP"], 5);
        }
    }
}