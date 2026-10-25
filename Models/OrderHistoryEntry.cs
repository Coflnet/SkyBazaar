using System;

namespace Coflnet.Sky.SkyBazaar.Models;

/// <summary>An order from the order_book history export. Deliberately has no player name.</summary>
public class OrderHistoryEntry
{
    /// <summary>When the order was placed.</summary>
    public DateTime Timestamp { get; set; }
    public string UserId { get; set; }
    public bool IsSell { get; set; }
    public double PricePerUnit { get; set; }
    public int Amount { get; set; }
    public int Filled { get; set; }
    public int? Claimed { get; set; }
    public bool IsExpired { get; set; }
    /// <summary>When the order was closed (cancelled/expired/claimed away); null while open.</summary>
    public DateTime? ClosedAt { get; set; }
    public bool? IsEstimate { get; set; }
    /// <summary>First time the order was persisted as fully filled; null if unknown.</summary>
    public DateTime? FilledAt { get; set; }
}
