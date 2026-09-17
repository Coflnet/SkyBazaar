using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Coflnet.Sky.SkyAuctionTracker.Services;
using Coflnet.Sky.SkyBazaar.Models;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace Coflnet.Sky.SkyAuctionTracker.Controllers
{
    /// <summary>
    /// OrderBook controller
    /// </summary>
    /// <returns></returns>
    /// 
    [ApiController]
    [Route("[controller]")]
    public class OrderBookController : ControllerBase
    {
        private readonly OrderBookService service;

        /// <summary>
        /// Creates a new instance of <see cref="OrderBookController"/>
        /// </summary>
        /// <param name="service"></param>
        public OrderBookController(OrderBookService service)
        {
            this.service = service;
        }

        private ObjectResult Loading()
        {
            Response.Headers.RetryAfter = "10";
            return StatusCode(503, new { code = "bazaar_loading", message = "Order matching is loading; retry in 10 seconds" });
        }

        /// <summary>Gets an authoritative user snapshot and rebuilds its Redis cache.</summary>
        [HttpGet("user/{userId}")]
        public async Task<IActionResult> GetUserOrders(string userId)
        {
            if (!service.IsReady) return Loading();
            return Content(await service.GetSnapshot(userId), "application/json");
        }

        /// <summary>
        /// Gets the order book for a specific item
        /// </summary>
        /// <param name="itemTag"></param>
        /// <returns></returns>
        [HttpGet]
        [Route("{itemTag}")]
        public async Task<OrderBook> GetOrderBook(string itemTag)
        {
            return await service.GetOrderBook(itemTag);
        }

        /// <summary>
        /// Adds an order to the order book
        /// </summary>
        /// <param name="order"></param>
        [HttpPost]
        public async Task<IActionResult> AddOrder(OrderEntry order)
        {
            if (!service.IsReady) return Loading();
            order.IsVerfified = false;
            await service.AddOrder(order);
            return Ok();
        }

        /// <summary>
        /// Removes and order from the order book
        /// </summary>
        [HttpDelete]
        public async Task<IActionResult> RemoveOrder(string itemTag, string userId, DateTime timestamp)
        {
            if (!service.IsReady) return Loading();
            await service.RemoveOrder(itemTag, userId, timestamp);
            return Ok();
        }

        /// <summary>
        /// Marks an order as filled so it won't trigger outbid notifications
        /// </summary>
        [HttpPost]
        [Route("filled")]
        public async Task<IActionResult> MarkOrderFilled(string itemTag, string userId, double pricePerUnit, int amount)
        {
            if (!service.IsReady) return Loading();
            await service.MarkOrderFilled(itemTag, userId, pricePerUnit, amount);
            return Ok();
        }

        /// <summary>
        /// Updates the in-memory order book with external data.
        /// Ignores updates that are older than the last Kafka update or in the future.
        /// Each side (buy/sell) is ignored when empty.
        /// </summary>
        /// <param name="update">The order book update with timestamp</param>
        /// <returns>True if the update was applied, false if it was ignored</returns>
        [HttpPost]
        [Route("update")]
        public async Task<bool> UpdateOrderBook([FromBody] OrderBookUpdate update)
        {
            return await service.UpdateOrderBook(update);
        }

        /// <summary>Matches an instant buy reported in chat without waiting for the public API.</summary>
        [HttpPost("instant-buy")]
        public async Task<IActionResult> InstantBuy(InstantBuyObservation observation)
        {
            if (string.IsNullOrWhiteSpace(observation.ItemTag) || observation.Amount <= 0
                || !double.IsFinite(observation.Coins) || observation.Coins <= 0)
                return BadRequest();
            return Ok(await service.ObserveInstantBuy(observation));
        }

        /// <summary>Reconciles a player's orders observed through a description upload.</summary>
        [HttpPost("player")]
        public async Task<IActionResult> ObservePlayerOrders(PlayerOrderObservation observation)
        {
            if (!service.IsReady) return Loading();
            if (string.IsNullOrWhiteSpace(observation.UserId) || string.IsNullOrWhiteSpace(observation.PlayerName)
                || observation.Timestamp > DateTime.UtcNow || observation.Orders == null
                || observation.Orders.Exists(o => o == null || string.IsNullOrWhiteSpace(o.ItemId) || o.Amount <= 0))
                return BadRequest(new { code = "invalid_player_orders", message = "Player orders require a user, player name, non-future observation timestamp, item IDs and positive amounts" });
            await service.ObservePlayerOrders(observation);
            return Ok();
        }

        /// <summary>
        /// Batch lookup of order books for multiple items
        /// </summary>
        /// <param name="itemTags">List of item tags to lookup</param>
        /// <returns>Dictionary mapping item tags to their current order books</returns>
        [HttpPost]
        [Route("batch")]
        public async Task<Dictionary<string, OrderBook>> GetOrderBooks([FromBody] List<string> itemTags)
        {
            return await service.GetOrderBooks(itemTags);
        }
    }
}
