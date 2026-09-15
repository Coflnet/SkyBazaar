extern alias CoflCore;

using System;
using System.IO;
using System.Reflection;
using Coflnet.Sky.SkyAuctionTracker.Services;
using Coflnet.Sky.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Prometheus;
using Coflnet.Sky.Items.Client.Api;
using Coflnet.Sky.EventBroker.Client.Api;
using CoflCore::Coflnet.Core;
using StackExchange.Redis;

namespace Coflnet.Sky.SkyAuctionTracker
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllers().AddJsonOptions(options =>
            {
                options.JsonSerializerOptions.DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingDefault;
            });

            // Replace with your server version and type.
            // Use 'MariaDbServerVersion' for MariaDB.
            // Alternatively, use 'ServerVersion.AutoDetect(connectionString)'.
            // For common usages, see pull request #1233.
            var serverVersion = new MariaDbServerVersion(new Version(Configuration["MARIADB_VERSION"]));

            // Replace 'YourDbContext' with the name of your own DbContext derived class.
            services.AddDbContext<HypixelContext>(
                dbContextOptions => dbContextOptions
                    .UseMySql(Configuration["DBCONNECTION"], serverVersion)
                    .EnableSensitiveDataLogging() // <-- These two calls are optional but help
                    .EnableDetailedErrors()       // <-- with debugging (remove for production).
            );
            services.AddHostedService<AggregationService>();
            if (Configuration["OLD_CASSANDRA:HOSTS"] != null)
            {
                services.AddSingleton<MigrationService>();
                services.AddHostedService(d => d.GetService<MigrationService>());
            }
            else
                services.AddHostedService<BazaarBackgroundService>();
            services.AddCoflnetCore();
            // Export asynchronously when configured; tracing must not add a startup dependency.
            if (!string.IsNullOrWhiteSpace(Configuration["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"]))
            {
                services.AddJaeger(Configuration, Configuration.GetValue("BAZAAR_TRACE_SAMPLE_RATE", 0.01));
                services.AddOpenTelemetry().WithTracing(b => b.AddSource(BazaarTelemetry.SourceName));
            }
            services.AddSingleton<BazaarService>();
            services.AddResponseCaching();
            services.AddResponseCompression();
            services.AddMemoryCache();
            services.AddSingleton(d => ConnectionMultiplexer.Connect(Configuration["SETTINGS_REDIS_HOST"]));
            services.AddSingleton<IConnectionMultiplexer>(sp => sp.GetRequiredService<ConnectionMultiplexer>());
            services.AddKeyedSingleton<IConnectionMultiplexer>("bazaar", (sp, key) => {
                var options = ConfigurationOptions.Parse(Configuration["EVENTS_REDIS_HOST"] ?? "sky-event-broker-redis");
                options.AbortOnConnectFail = false;
                options.ConnectTimeout = 1000;
                options.AsyncTimeout = 1000;
                options.ConnectRetry = 0;
                return ConnectionMultiplexer.Connect(options);
            });
            services.AddSingleton<BazaarOrderPublisher>();
            services.AddHostedService(sp => sp.GetRequiredService<BazaarOrderPublisher>());
            services.AddSingleton<OrderBookService>();
            services.AddSingleton<IBlobHistoryStore, S3BlobHistoryStore>();
            if (bool.TryParse(Configuration["HISTORY_ARCHIVE:ENABLED"], out var archiveEnabled) && archiveEnabled)
                services.AddHostedService<HistoryArchiveService>();
            services.AddSingleton<ISessionContainer>(d => d.GetRequiredService<BazaarService>());
            services.AddSingleton<IItemsApi, ItemsApi>(d =>
            {
                return new ItemsApi(Configuration["ITEMS_BASE_URL"]);
            });
            services.AddSingleton<IMessageApi>(d =>
            {
                return new MessageApi(Configuration["EVENTS_BASE_URL"]);
            });
        }

        /// <summary>
        /// This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        /// </summary>
        /// <param name="app"></param>
        /// <param name="env"></param>
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            var logger = app.ApplicationServices.GetRequiredService<ILogger<Startup>>();
            if (string.IsNullOrWhiteSpace(Configuration["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"]))
                logger.LogWarning("Bazaar trace export disabled: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT is not configured; logs and metrics remain active");
            else
                logger.LogInformation("Bazaar tracing enabled for {SourceName}, root sample rate {SampleRate}",
                    BazaarTelemetry.SourceName, Configuration.GetValue("BAZAAR_TRACE_SAMPLE_RATE", 0.01));
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            app.UseSwagger();
            app.UseSwaggerUI(c =>
            {
                c.SwaggerEndpoint("/swagger/v1/swagger.json", "SkyBazaar v1");
                c.RoutePrefix = "api";
            });

            app.UseResponseCaching();
            app.UseResponseCompression();

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapMetrics();
                endpoints.MapControllers();
            });
        }
    }
}
