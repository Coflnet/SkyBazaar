using System.Threading;
using System.Threading.Tasks;
using Coflnet.Sky.SkyBazaar.Models;

namespace Coflnet.Sky.SkyAuctionTracker.Services
{
    /// <summary>
    /// Abstraction over the cold-tier blob storage for 30-day minute archive blocks.
    /// Implementations are expected to be thread-safe.
    /// </summary>
    public interface IBlobHistoryStore
    {
        /// <summary>True if cold-tier writes/reads should be attempted (i.e. config is complete).</summary>
        bool IsEnabled { get; }

        /// <summary>
        /// Read the archive block for (productId, bucketId). Returns null if the blob does not exist.
        /// Other errors propagate as exceptions.
        /// </summary>
        Task<ArchivedMinuteBlock> ReadBlobAsync(string productId, int bucketId, CancellationToken cancellationToken = default);

        /// <summary>Write (or overwrite) the archive block for (productId, bucketId).</summary>
        Task WriteBlobAsync(ArchivedMinuteBlock block, CancellationToken cancellationToken = default);

        /// <summary>True if the blob for (productId, bucketId) already exists in cold storage.</summary>
        Task<bool> BlobExistsAsync(string productId, int bucketId, CancellationToken cancellationToken = default);
    }
}
