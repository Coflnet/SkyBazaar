using System;
using System.Collections.Generic;
using System.Linq;

namespace Coflnet.Sky.SkyBazaar.Models;

public class OrderBook
{
    /// <summary>
    /// all buy orders (biggest is best)
    /// </summary>
    public List<OrderEntry> Buy { get; set; } = new();
    /// <summary>
    /// all sell orders (smallest is best)
    /// </summary>
    public List<OrderEntry> Sell { get; set; } = new();

    /// <summary>
    /// Returns true if there was sombebody outbid by the new order
    /// </summary>
    /// <param name="entry"></param>
    /// <param name="outbid"></param>
    /// <returns></returns>
    public bool TryGetOutbid(OrderEntry entry, out OrderEntry outbid)
    {
        outbid = null;
        if (entry.IsSell)
        {
            foreach (var item in Sell.ToList().OrderByDescending(o => o.PricePerUnit).Take(5).ToList())
            {
                if (Math.Round(item.PricePerUnit, 1) > Math.Round(entry.PricePerUnit, 1) && item.UserId != null)
                {
                    outbid = item;
                    return true;
                }
            }
        }
        else
        {
            foreach (var item in Buy.ToList().OrderByDescending(o => o.PricePerUnit).Take(5).ToList())
            {
                if (Math.Round(item.PricePerUnit, 1) < Math.Round(entry.PricePerUnit, 1) && item.UserId != null)
                {
                    outbid = item;
                    return true;
                }
            }
        }
        return false;
    }

    /// <summary>
    /// Returns all orders that are outbid by the new entry and haven't been notified yet
    /// For sell orders: returns orders with higher price (worse)
    /// For buy orders: returns orders with lower price (worse)
    /// </summary>
    /// <param name="entry">The new order being added</param>
    /// <returns>List of orders that have been outbid and should be notified</returns>
    public List<OrderEntry> GetAllOutbidOrders(OrderEntry entry)
    {
        var outbidOrders = new List<OrderEntry>();
        
        if (entry.IsSell)
        {
            // For sell orders, find all orders with higher price that haven't been notified
            outbidOrders = Sell
                .Where(o => o.UserId != null 
                    && !o.HasBeenNotified 
                    && Math.Round(o.PricePerUnit, 1) > Math.Round(entry.PricePerUnit, 1))
                .OrderBy(o => o.PricePerUnit) // Start with closest to new price
                .ToList();
        }
        else
        {
            // For buy orders, find all orders with lower price that haven't been notified
            outbidOrders = Buy
                .Where(o => o.UserId != null 
                    && !o.HasBeenNotified 
                    && Math.Round(o.PricePerUnit, 1) < Math.Round(entry.PricePerUnit, 1))
                .OrderByDescending(o => o.PricePerUnit) // Start with closest to new price
                .ToList();
        }
        
        return outbidOrders;
    }

    internal bool Remove(OrderEntry order)
    {
        var side = order.IsSell ? this.Sell : this.Buy;
        var onBook = side.Where(o => o.Timestamp == order.Timestamp && o.PlayerName != null).FirstOrDefault();
        return side.Remove(onBook);

    }
}