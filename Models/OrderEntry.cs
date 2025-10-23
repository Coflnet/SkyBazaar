using System;

namespace Coflnet.Sky.SkyBazaar.Models;
public class OrderEntry
{
    public int Amount { get; set; }
    public double PricePerUnit { get; set; }
    public string PlayerName { get; set; }
    public string UserId { get; set; }
    public bool IsSell { get; set; }
    public DateTime Timestamp { get; set; }
    public string ItemId { get; set; }
    /// <summary>
    /// Has the order been verified via api?
    /// </summary>
    public bool IsVerfified { get; set; }
    public int Filled { get; set; }
    /// <summary>
    /// Has the user been notified about being outbid/undercut for this order?
    /// </summary>
    public bool HasBeenNotified { get; set; }

    public override bool Equals(object obj)
    {
        return obj is OrderEntry entry &&
               Amount == entry.Amount &&
               PricePerUnit == entry.PricePerUnit &&
               PlayerName == entry.PlayerName &&
               UserId == entry.UserId &&
               IsSell == entry.IsSell &&
               Timestamp == entry.Timestamp &&
               ItemId == entry.ItemId &&
               IsVerfified == entry.IsVerfified &&
               Filled == entry.Filled;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Amount, PricePerUnit, UserId, IsSell, Timestamp, ItemId);
    }
}
