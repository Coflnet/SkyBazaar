using MessagePack;
using Newtonsoft.Json;

namespace Coflnet.Sky.SkyBazaar.Models
{
    /// <summary>
    /// Special version of 
    /// </summary>
    public class AggregatedQuickStatus : StorageQuickStatus
    {

        /// <summary>
        /// The biggest buy price in this aggregated timespan
        /// </summary>
        /// <value></value>
        [IgnoreMember]
        [JsonProperty("maxBuy")]
        public float MaxBuy { get; set; }
        /// <summary>
        /// The biggest sell price in this aggregated timespan
        /// </summary>
        /// <value></value>
        [IgnoreMember]
        [JsonProperty("maxSell")]
        public float MaxSell { get; set; }
        /// <summary>
        /// The smalest buy price in this aggregated timespan
        /// </summary>
        /// <value></value>
        [IgnoreMember]
        [JsonProperty("minBuy")]
        public float MinBuy { get; set; }
        /// <summary>
        /// The smalest sell price in this aggregated timespan
        /// </summary>
        /// <value></value>
        [IgnoreMember]
        [JsonProperty("minSell")]
        public float MinSell { get; set; }

        public AggregatedQuickStatus(StorageQuickStatus status) : base(status)
        {
        }

        public AggregatedQuickStatus()
        {
        }
    }
}