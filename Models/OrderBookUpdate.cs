using System;
using System.Collections.Generic;

namespace Coflnet.Sky.SkyBazaar.Models;

public class OrderBookUpdate
{
    /// <summary>
    /// The item tag/ID for the order book
    /// </summary>
    public string ItemTag { get; set; }
    
    /// <summary>
    /// Timestamp of this order book data
    /// </summary>
    public DateTime Timestamp { get; set; }
    
    /// <summary>
    /// Buy orders (can be null/empty to ignore)
    /// </summary>
    public List<OrderEntry> BuyOrders { get; set; }
    
    /// <summary>
    /// Sell orders (can be null/empty to ignore)
    /// </summary>
    public List<OrderEntry> SellOrders { get; set; }
}
