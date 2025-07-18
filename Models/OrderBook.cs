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

    internal bool Remove(OrderEntry order)
    {
        var side = order.IsSell ? this.Sell : this.Buy;
        var onBook = side.Where(o => o.Timestamp == order.Timestamp && o.PlayerName != null).FirstOrDefault();
        return side.Remove(onBook);

    }
}