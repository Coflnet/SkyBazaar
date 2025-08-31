using System;
using System.Collections.Generic;
using Cassandra.Mapping.Attributes;
using MessagePack;
using Newtonsoft.Json;

namespace Coflnet.Sky.SkyBazaar.Models
{
    public class SplitStorageQuickStatus : StorageQuickStatus
    {
        public short WeekId { get; set; }
    }
    public class StorageQuickStatus
    {
        [Key(0)]
        [JsonProperty("productId")]
        [PartitionKey]
        public string ProductId { get; set; }
        [Key(1)]
        [JsonProperty("buyPrice")]
        public double BuyPrice { get; set; }
        [Key(2)]
        [JsonProperty("buyVolume")]
        public long BuyVolume { get; set; }
        [Key(3)]
        [JsonProperty("buyMovingWeek")]
        public long BuyMovingWeek { get; set; }
        [Key(4)]
        [JsonProperty("buyOrderCount")]
        public int BuyOrdersCount { get; set; }
        [Key(5)]
        [JsonProperty("sellPrice")]
        public double SellPrice { get; set; }
        [Key(6)]
        [JsonProperty("sellVolume")]
        public long SellVolume { get; set; }
        [Key(7)]
        [JsonProperty("sellMovingWeek")]
        public long SellMovingWeek { get; set; }
        [Key(8)]
        [JsonProperty("sellOrderCount")]
        public int SellOrdersCount { get; set; }
        [Key(9)]
        [JsonProperty("timestamp")]
        [ClusteringKey]
        public DateTime TimeStamp { get; set; }
        [IgnoreMember]
        [JsonIgnore]
        [System.Text.Json.Serialization.JsonIgnore]
        public byte[] SerialisedBuyOrders { get; set; }
        [IgnoreMember]
        [JsonIgnore]
        [System.Text.Json.Serialization.JsonIgnore]
        public byte[] SerialisedSellOrders { get; set; }
        [IgnoreMember]
        [JsonProperty("refId")]
        [System.Text.Json.Serialization.JsonIgnore]
        public long ReferenceId { get; set; }

        [Ignore]
        [IgnoreMember]
        [JsonProperty("buyOrders")]
        public IEnumerable<Order> BuyOrders
        {
            get
            {
                return MessagePackSerializer.Deserialize<IEnumerable<BuyOrder>>(SerialisedBuyOrders);
            }
            set
            {
                if (SerialisedBuyOrders == null && value != null)
                    SerialisedBuyOrders = MessagePackSerializer.Serialize(value);
            }
        }
        [Ignore]
        [IgnoreMember]
        [JsonProperty("sellOrders")]
        public IEnumerable<Order> SellOrders
        {
            get
            {
                return MessagePackSerializer.Deserialize<IEnumerable<SellOrder>>(SerialisedSellOrders);
            }
            set
            {
                if (SerialisedSellOrders == null && value != null)
                    SerialisedSellOrders = MessagePackSerializer.Serialize(value);
            }
        }

        public StorageQuickStatus() { }

        public StorageQuickStatus(StorageQuickStatus status)
        {
            ProductId = status.ProductId;
            BuyPrice = status.BuyPrice;
            BuyVolume = status.BuyVolume;
            BuyMovingWeek = status.BuyMovingWeek;
            BuyOrdersCount = status.BuyOrdersCount;
            SellPrice = status.SellPrice;
            SellVolume = status.SellVolume;
            SellMovingWeek = status.SellMovingWeek;
            SellOrdersCount = status.SellOrdersCount;
            TimeStamp = status.TimeStamp;
            ReferenceId = status.ReferenceId;
            SerialisedBuyOrders = status.SerialisedBuyOrders;
            SerialisedSellOrders = status.SerialisedSellOrders;
        }
    }

    public class BuyOrder : Order
    {
        public BuyOrder() { }
    }

    public class SellOrder : Order
    {
        public SellOrder() { }
    }


    [MessagePackObject]
    public class Order
    {
        public Order() { }
        [Key(0)]
        [JsonProperty("amount")]
        public int Amount { get; set; }
        [Key(1)]
        [JsonProperty("pricePerUnit")]
        public double PricePerUnit { get; set; }
        [Key(2)]
        [JsonProperty("orders")]
        public short Orders { get; set; }

        public bool ValueSame(Order order)
        {
            return Amount == order.Amount
                && PricePerUnit == order.PricePerUnit
                && Orders == order.Orders;
        }
    }
}