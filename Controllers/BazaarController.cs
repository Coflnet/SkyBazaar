using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using System.Collections;
using System.Collections.Generic;
using Coflnet.Sky.SkyAuctionTracker.Services;
using dev;
using Coflnet.Sky.SkyBazaar.Models;
using Coflnet.Sky.Items.Client.Api;

namespace Coflnet.Sky.SkyAuctionTracker.Controllers
{
    /// <summary>
    /// Main Controller handling tracking
    /// </summary>
    [ApiController]
    [Route("api/[controller]")]
    public class BazaarController : ControllerBase
    {
        private readonly BazaarService service;
        private readonly IItemsApi itemsApi;

        /// <summary>
        /// Creates a new instance of <see cref="BazaarController"/>
        /// </summary>
        /// <param name="service"></param>
        /// <param name="itemsApi"></param>
        public BazaarController(BazaarService service, IItemsApi itemsApi)
        {
            this.service = service;
            this.itemsApi = itemsApi;
        }

        /// <summary>
        /// Gets the latest status for an item
        /// </summary>
        /// <param name="itemId"></param>
        /// <returns></returns>
        [Route("{itemId}/status")]
        [ResponseCache(Duration = 10, Location = ResponseCacheLocation.Any, NoStore = false)]
        [HttpGet]
        public async Task<SkyBazaar.Models.StorageQuickStatus> GetStatus(string itemId)
        {
            var entries = (await service.GetStatus(itemId, DateTime.UtcNow - TimeSpan.FromSeconds(520), DateTime.UtcNow, 1)).ToList();
            return entries.FirstOrDefault();
        }
        /// <summary>
        /// Gets the latest status for an item
        /// </summary>
        /// <param name="itemId"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        [Route("{itemId}/data")]
        [ResponseCache(Duration = 10, Location = ResponseCacheLocation.Any, NoStore = false, VaryByQueryKeys = new string[] { "start", "end" })]
        [HttpGet]
        public async Task<IEnumerable<SkyBazaar.Models.AggregatedQuickStatus>> GetData(string itemId, DateTime start, DateTime end)
        {
            var entries = await service.GetStatus(itemId, start, end, 100);
            return entries;
        }

        /// <summary>
        /// Gets a snapshot of the order book at a specific time
        /// </summary>
        /// <param name="itemId"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        [Route("{itemId}/snapshot")]
        [ResponseCache(Duration = 20, Location = ResponseCacheLocation.Any, NoStore = false, VaryByQueryKeys = new string[] { "time" })]
        [HttpGet]
        public async Task<SkyBazaar.Models.StorageQuickStatus> GetClosestTo(string itemId, DateTime time = default)
        {
            if (time == default)
                time = DateTime.UtcNow;
            var entries = await service.GetStatus(itemId, time - TimeSpan.FromMinutes(1), time + TimeSpan.FromSeconds(9), 1);
            var match = entries.FirstOrDefault();
            if (match != default)
                return match;
            return (await service.GetStatus(itemId, time - TimeSpan.FromHours(1), time + TimeSpan.FromSeconds(19), 1)).FirstOrDefault();
        }
        /// <summary>
        /// Gets the latest status for an item
        /// </summary>
        /// <param name="itemId"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        [Route("{itemId}/history")]
        [ResponseCache(Duration = 120, Location = ResponseCacheLocation.Any, NoStore = false, VaryByQueryKeys = new string[] { "start", "end" })]
        [HttpGet]
        public async Task<IEnumerable<GraphResult>> GetHistoryGraph(string itemId, DateTime start, DateTime end)
        {
            var entries = await service.GetStatus(itemId, start, end, 500);
            var result = entries.ToList();

            Console.WriteLine(result.Count());

            return result.Select(a =>
            {
                return new GraphResult()
                {
                    Buy = a.BuyOrders.FirstOrDefault()?.PricePerUnit ?? a.BuyPrice,
                    MaxBuy = a.MaxBuy,
                    MinBuy = a.MinBuy,
                    MinSell = a.MinSell,
                    Sell = a.SellOrders.FirstOrDefault()?.PricePerUnit ?? a.SellPrice,
                    SellVolume = a.SellVolume,
                    BuyVolume = a.BuyVolume,
                    Timestamp = a.TimeStamp,
                    MaxSell = a.MaxSell,
                    BuyMovingWeek = a.BuyMovingWeek,
                    SellMovingWeek = a.SellMovingWeek

                };
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        [Route("prices")]
        [ResponseCache(Duration = 20, Location = ResponseCacheLocation.Any, NoStore = false)]
        [HttpGet]
        public async Task<IEnumerable<ItemPrice>> GetAllPrices()
        {
            var tags = await itemsApi.ItemsBazaarTagsGetAsync();
            return await service.GetCurrentPrices(tags);
        }
    }
}
