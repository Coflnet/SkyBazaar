using System;

namespace Coflnet.Sky.SkyBazaar.Models;

public class InstantBuyObservation
{
    public string ItemTag { get; set; }
    public DateTime Timestamp { get; set; }
    public int Amount { get; set; }
    public double Coins { get; set; }
}
