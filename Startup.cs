extern alias CoflCore;

using System;
using System.IO;
using System.Reflection;
using Coflnet.Sky.SkyAuctionTracker.Services;
using Coflnet.Sky.Core;
using Jaeger.Samplers;
using Jaeger.Senders;
using Jaeger.Senders.Thrift;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using OpenTracing;
using OpenTracing.Util;
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
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "SkyBazaar", Version = "v1" });
                // Set the comments path for the Swagger JSON and UI.
                var xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
                var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
                c.IncludeXmlComments(xmlPath);
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
            services.AddHostedService<BazaarBackgroundService>();
            services.AddHostedService<AggregationService>();
            if (Configuration["OLD_CASSANDRA:HOSTS"] != null)
            {
                services.AddSingleton<MigrationService>();
                services.AddHostedService(d => d.GetService<MigrationService>());
            }
            services.AddCoflnetCore();
            // services.AddJaeger(Configuration);
            services.AddSingleton<BazaarService>();
            services.AddResponseCaching();
            services.AddMemoryCache();
            services.AddSingleton(d => ConnectionMultiplexer.Connect(Configuration["SETTINGS_REDIS_HOST"]));
            services.AddSingleton<OrderBookService>();
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
