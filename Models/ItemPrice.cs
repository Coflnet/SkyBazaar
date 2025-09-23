namespace Coflnet.Sky.SkyBazaar.Models
{
    public class ItemPrice
    {
        public string ProductId { get; set; }
        public double BuyPrice { get; set; }
        public int DailyBuyVolume { get; set; }
        public int DailySellVolume { get; set; }
        public double SellPrice { get; set; }
    }
}