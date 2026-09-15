using System;
using System.Collections.Generic;

namespace Coflnet.Sky.SkyBazaar.Models;

public class PlayerOrderObservation
{
    public string UserId { get; set; }
    public string PlayerName { get; set; }
    public DateTime Timestamp { get; set; }
    public List<OrderEntry> Orders { get; set; } = new();
}
