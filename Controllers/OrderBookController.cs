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
        public async Task AddOrder(OrderEntry order)
        {
            order.IsVerfified = false;
            await service.AddOrder(order);
        }

        /// <summary>
        /// Removes and order from the order book
        /// </summary>
        [HttpDelete]
        public async Task RemoveOrder(string itemTag, string userId, DateTime timestamp)
        {
            await service.RemoveOrder(itemTag, userId, timestamp);
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
