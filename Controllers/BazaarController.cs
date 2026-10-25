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
using Cassandra;
using System.Threading;

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
        private readonly DepthExportService depthExport;

        /// <summary>
        /// Creates a new instance of <see cref="BazaarController"/>
        /// </summary>
        /// <param name="service"></param>
        /// <param name="itemsApi"></param>
        /// <param name="depthExport"></param>
        public BazaarController(BazaarService service, IItemsApi itemsApi, DepthExportService depthExport)
        {
            this.service = service;
            this.itemsApi = itemsApi;
            this.depthExport = depthExport;
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
        /// <param name="smallestResolution"></param>
        /// <param name="includeArchivedOrderbook"></param>
        /// <returns></returns>
        [Route("{itemId}/data")]
        [ResponseCache(Duration = 10, Location = ResponseCacheLocation.Any, NoStore = false, VaryByQueryKeys = new string[] { "start", "end", "smallestResolution", "includeArchivedOrderbook" })]
        [HttpGet]
        public async Task<IEnumerable<SkyBazaar.Models.AggregatedQuickStatus>> GetData(string itemId, DateTime start, DateTime end, bool smallestResolution = false, bool includeArchivedOrderbook = false)
        {
            var points = smallestResolution ? 3 * 60 * 24 * 30 : 100;
            // The public export is the only caller of this data endpoint. Keep stronger reads
            // here so graph, snapshot, and live status requests retain their normal consistency.
            var entries = await service.GetStatus(itemId, start, end, points, smallestResolution, includeArchivedOrderbook, ConsistencyLevel.Quorum);
            return entries;
        }

        private static async IAsyncEnumerable<DepthSnapshot> Remaining(IAsyncEnumerator<DepthSnapshot> rows, bool hasCurrent)
        {
            while (hasCurrent)
            {
                yield return rows.Current;
                hasCurrent = await rows.MoveNextAsync();
            }
        }

        private static DateTime? AsUtc(DateTime? time) => time == null ? null
            : time.Value.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(time.Value, DateTimeKind.Utc) : time.Value.ToUniversalTime();

        /// <summary>
        /// Compact bulk export of order book depth history ("depth-compact-v1"), ascending by time, streamed
        /// at the finest stored cadence. At most 31 days per call, 1 to 30 levels per side, and a global
        /// concurrency limit (HTTP 429 with Retry-After when saturated).
        /// </summary>
        /// <param name="itemId"></param>
        /// <param name="start">Exclusive window start (required)</param>
        /// <param name="end">Inclusive window end (required)</param>
        /// <param name="levels">Levels per side, 1 to 30</param>
        [Route("{itemId}/depth/export")]
        [HttpGet]
        public async Task<IActionResult> ExportDepth(string itemId, DateTime? start, DateTime? end, int levels = DepthExportService.DefaultLevels)
        {
            start = AsUtc(start);
            end = AsUtc(end);
            var error = DepthExportService.Validate(start, end, levels);
            if (error != null)
                return BadRequest(new { code = "invalid_depth_export", message = error });
            if (!depthExport.TryEnter())
            {
                Response.Headers.RetryAfter = "5";
                return StatusCode(429, new { code = "depth_export_busy", message = "Too many concurrent depth exports; retry shortly" });
            }
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(HttpContext.RequestAborted);
                cts.CancelAfter(depthExport.Timeout);
                IAsyncEnumerator<DepthSnapshot> rows = null;
                try
                {
                    rows = depthExport.Read(itemId, start.Value, end.Value, cts.Token).GetAsyncEnumerator(cts.Token);
                    // Pull the first row before any header goes out, so failures before data are a clean error status.
                    var hasFirst = await rows.MoveNextAsync();
                    Response.ContentType = "application/json; charset=utf-8";
                    HttpContext.Features.Get<Microsoft.AspNetCore.Http.Features.IHttpResponseBodyFeature>()?.DisableBuffering();
                    await DepthExportWriter.WriteAsync(Response.Body, Remaining(rows, hasFirst), levels, cts.Token);
                }
                catch (Exception e) when (!Response.HasStarted)
                {
                    if (e is OperationCanceledException)
                        return StatusCode(HttpContext.RequestAborted.IsCancellationRequested ? 499 : 504,
                            new { code = "depth_export_cancelled", message = "Depth export timed out or was cancelled" });
                    return StatusCode(500, new { code = "depth_export_failed", message = "Depth export failed before any data was sent" });
                }
                catch (Exception)
                {
                    HttpContext.Abort(); // a truncated body must not look like a complete document
                }
                finally
                {
                    if (rows != null)
                        await rows.DisposeAsync();
                }
                return new EmptyResult();
            }
            finally
            {
                depthExport.Exit();
            }
        }

        /// <summary>
        /// Gets a snapshot of the order book at a specific time
        /// </summary>
        /// <param name="itemId"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        [Route("{itemId}/snapshot")]
        [ResponseCache(Duration = 10, Location = ResponseCacheLocation.Any, NoStore = false, VaryByQueryKeys = new string[] { "time" })]
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
        /// <param name="smallestResolution"></param>
        /// <returns></returns>
        [Route("{itemId}/history")]
        [ResponseCache(Duration = 120, Location = ResponseCacheLocation.Any, NoStore = false, VaryByQueryKeys = new string[] { "start", "end", "smallestResolution" })]
        [HttpGet]
        public async Task<IEnumerable<GraphResult>> GetHistoryGraph(string itemId, DateTime start, DateTime end, bool smallestResolution = false)
        {
            var points = smallestResolution ? 3 * 60 * 24 * 14 : 2000;
            var entries = await service.GetStatus(itemId, start, end, points, smallestResolution);
            var result = entries.ToList();

            return result.Select(a =>
            {
                return new GraphResult()
                {
                    Buy = a.BuyOrders?.FirstOrDefault()?.PricePerUnit ?? a.BuyPrice,
                    MaxBuy = a.MaxBuy,
                    MinBuy = a.MinBuy,
                    MinSell = a.MinSell,
                    Sell = a.SellOrders?.FirstOrDefault()?.PricePerUnit ?? a.SellPrice,
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

        [Route("movement")]
        [ResponseCache(Duration = 20, Location = ResponseCacheLocation.Any, NoStore = false, VaryByQueryKeys = new string[] { "hours", "usebuyOrders" })]
        [HttpGet]
        public async Task<IEnumerable<ItemPriceMovement>> GetMovement(int hours = 24, bool usebuyOrders = false)
        {
            return await service.GetMovement(hours, usebuyOrders);
        }

    }
}
