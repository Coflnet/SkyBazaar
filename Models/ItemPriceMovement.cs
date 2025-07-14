namespace Coflnet.Sky.SkyBazaar.Models;

/// <summary>
/// Represents a comparison between current and previous item prices.
/// Used for displaying change in the market
/// </summary>
public class ItemPriceMovement
{
    public string ItemId { get; set; }
    public double PreviousPrice { get; set; }
    public double CurrentPrice { get; set; }
}