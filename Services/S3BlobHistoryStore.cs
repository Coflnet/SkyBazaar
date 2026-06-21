using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Coflnet.Sky.SkyBazaar.Models;
using MessagePack;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.SkyAuctionTracker.Services
{
    /// <summary>
    /// S3-compatible implementation of <see cref="IBlobHistoryStore"/>. Works with AWS S3,
    /// Cloudflare R2, Hetzner Object Storage and MinIO via a custom endpoint and path style.
    /// Blobs are MessagePack-serialized <see cref="ArchivedMinuteBlock"/> values compressed
    /// with Brotli at its highest level for maximum reduction.
    /// </summary>
    public class S3BlobHistoryStore : IBlobHistoryStore, IDisposable
    {
        private const string KeyPrefix = "bazaar/min5/v1/";
        private const string ContentType = "application/x-msgpack+br";

        private readonly ILogger<S3BlobHistoryStore> logger;
        private readonly MemoryCache cache;
        private readonly IAmazonS3 client;
        private readonly string bucket;
        private readonly bool enabled;
        private readonly TimeSpan cacheTtl;
        private readonly long perEntryOverheadBytes;

        private static readonly Prometheus.Counter blobReadHits = Prometheus.Metrics.CreateCounter("sky_bazaar_blob_read_hits", "Cold-tier blob read returned data");
        private static readonly Prometheus.Counter blobReadMisses = Prometheus.Metrics.CreateCounter("sky_bazaar_blob_read_misses", "Cold-tier blob read returned 404");
        private static readonly Prometheus.Counter blobReadErrors = Prometheus.Metrics.CreateCounter("sky_bazaar_blob_read_errors", "Cold-tier blob read failed with an unexpected error");
        private static readonly Prometheus.Counter blobReadCacheHits = Prometheus.Metrics.CreateCounter("sky_bazaar_blob_read_cache_hits", "Cold-tier blob read served from in-process cache");
        private static readonly Prometheus.Counter blobWrites = Prometheus.Metrics.CreateCounter("sky_bazaar_blob_writes", "Cold-tier blob writes");
        private static readonly Prometheus.Counter blobWriteFailures = Prometheus.Metrics.CreateCounter("sky_bazaar_blob_write_failures", "Cold-tier blob write failures");
        private static readonly Prometheus.Histogram blobReadBytes = Prometheus.Metrics.CreateHistogram("sky_bazaar_blob_read_bytes", "Compressed cold-tier blob size on read",
            new Prometheus.HistogramConfiguration { Buckets = Prometheus.Histogram.ExponentialBuckets(256, 2, 14) });
        private static readonly Prometheus.Histogram blobWriteBytes = Prometheus.Metrics.CreateHistogram("sky_bazaar_blob_write_bytes", "Compressed cold-tier blob size on write",
            new Prometheus.HistogramConfiguration { Buckets = Prometheus.Histogram.ExponentialBuckets(256, 2, 14) });

        /// <inheritdoc/>
        public bool IsEnabled => enabled;

        /// <summary>DI constructor; pulls S3 config from <c>S3:*</c> keys.</summary>
        public S3BlobHistoryStore(IConfiguration config, ILogger<S3BlobHistoryStore> logger)
        {
            this.logger = logger;
            this.cacheTtl = TimeSpan.FromMinutes(ConfigInt(config, "S3:CACHE_SLIDING_TTL_MINUTES", 10));
            // Per-entry overhead used to charge each cache entry. The framework's MemoryCache
            // eviction uses per-entry Size + SizeLimit. We bill each entry as
            // compressed-bytes + this constant to also account for the materialized
            // ArchivedMinuteBlock object graph living in memory.
            this.perEntryOverheadBytes = ConfigInt(config, "S3:CACHE_PER_ENTRY_OVERHEAD_BYTES", 8 * 1024);

            var cacheSizeBytes = ConfigLong(config, "S3:CACHE_SIZE_BYTES", 200L * 1024 * 1024);
            // Dedicated MemoryCache so cold-tier reads cannot evict the rest of the app's caches.
            // SizeLimit is in "units" \u2014 we use bytes (compressed payload + overhead) as the unit.
            this.cache = new MemoryCache(new MemoryCacheOptions { SizeLimit = cacheSizeBytes });

            if (!bool.TryParse(config["S3:ENABLED"], out var isEnabled) || !isEnabled)
            {
                enabled = false;
                return;
            }

            var endpoint = config["S3:ENDPOINT"];
            var region = config["S3:REGION"];
            this.bucket = config["S3:BUCKET"];
            var accessKey = config["S3:ACCESS_KEY"];
            var secretKey = config["S3:SECRET_KEY"];
            var pathStyle = !bool.TryParse(config["S3:USE_PATH_STYLE"], out var ps) || ps;

            if (string.IsNullOrWhiteSpace(bucket) || string.IsNullOrWhiteSpace(accessKey) || string.IsNullOrWhiteSpace(secretKey))
            {
                logger.LogWarning("S3:ENABLED=true but S3:BUCKET/S3:ACCESS_KEY/S3:SECRET_KEY are not all set; cold-tier disabled.");
                enabled = false;
                return;
            }

            var s3Config = new AmazonS3Config
            {
                ForcePathStyle = pathStyle,
            };
            if (!string.IsNullOrWhiteSpace(endpoint))
                s3Config.ServiceURL = endpoint;
            if (!string.IsNullOrWhiteSpace(region))
                s3Config.RegionEndpoint = RegionEndpoint.GetBySystemName(region);

            this.client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey), s3Config);
            this.enabled = true;
            logger.LogInformation("S3 cold-tier enabled: bucket={Bucket} endpoint={Endpoint} pathStyle={PathStyle}", bucket, endpoint ?? "(aws default)", pathStyle);
        }

        private static string KeyFor(string productId, int bucketId)
        {
            // bucketId zero-padded so prefix listings stay sortable.
            return KeyPrefix + productId + "/" + bucketId.ToString("D6") + ".msgpack.br";
        }

        /// <inheritdoc/>
        public async Task<ArchivedMinuteBlock> ReadBlobAsync(string productId, int bucketId, CancellationToken cancellationToken = default)
        {
            if (!enabled)
                return null;
            var cacheKey = "blob:" + productId + ":" + bucketId;
            if (cache.TryGetValue(cacheKey, out ArchivedMinuteBlock cached))
            {
                blobReadCacheHits.Inc();
                return cached;
            }

            try
            {
                using var response = await client.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = bucket,
                    Key = KeyFor(productId, bucketId),
                }, cancellationToken).ConfigureAwait(false);
                using var responseStream = response.ResponseStream;
                using var brotli = new BrotliStream(responseStream, CompressionMode.Decompress);
                using var buffered = new MemoryStream();
                await brotli.CopyToAsync(buffered, cancellationToken).ConfigureAwait(false);
                blobReadBytes.Observe(response.ContentLength);
                buffered.Position = 0;
                var block = MessagePackSerializer.Deserialize<ArchivedMinuteBlock>(buffered, cancellationToken: cancellationToken);
                CacheBlock(cacheKey, block, response.ContentLength);
                blobReadHits.Inc();
                return block;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                blobReadMisses.Inc();
                return null;
            }
            catch (Exception ex)
            {
                blobReadErrors.Inc();
                logger.LogWarning(ex, "Cold-tier read failed for {Product} bucket {Bucket}", productId, bucketId);
                throw;
            }
        }

        /// <inheritdoc/>
        public async Task WriteBlobAsync(ArchivedMinuteBlock block, CancellationToken cancellationToken = default)
        {
            if (!enabled)
                throw new InvalidOperationException("S3 cold-tier is not enabled");
            if (block == null)
                throw new ArgumentNullException(nameof(block));

            byte[] payload;
            using (var raw = new DiskBackedStream())
            {
                using (var brotli = new BrotliStream(raw, CompressionLevel.SmallestSize, leaveOpen: true))
                {
                    MessagePackSerializer.Serialize(brotli, block, cancellationToken: cancellationToken);
                }
                payload = raw.ToArray();
            }

            try
            {
                using var ms = new MemoryStream(payload, writable: false);
                await client.PutObjectAsync(new PutObjectRequest
                {
                    BucketName = bucket,
                    Key = KeyFor(block.ProductId, block.BucketId),
                    InputStream = ms,
                    AutoCloseStream = false,
                    ContentType = ContentType,
                    // R2 and some other S3-compatible providers reject chunked SigV4 payload
                    // signing. We sign headers but not the body; integrity is still covered
                    // by HTTPS-in-transit and an optional Content-MD5 check (omitted to keep
                    // CPU low on already-Brotli-compressed payloads).
                    DisablePayloadSigning = true,
                }, cancellationToken).ConfigureAwait(false);
                blobWrites.Inc();
                blobWriteBytes.Observe(payload.Length);
                CacheBlock("blob:" + block.ProductId + ":" + block.BucketId, block, payload.Length);
            }
            catch (Exception ex)
            {
                blobWriteFailures.Inc();
                logger.LogWarning(ex, "Cold-tier write failed for {Product} bucket {Bucket}", block.ProductId, block.BucketId);
                throw;
            }
        }

        /// <inheritdoc/>
        public async Task<bool> BlobExistsAsync(string productId, int bucketId, CancellationToken cancellationToken = default)
        {
            if (!enabled)
                return false;
            try
            {
                await client.GetObjectMetadataAsync(new GetObjectMetadataRequest
                {
                    BucketName = bucket,
                    Key = KeyFor(productId, bucketId),
                }, cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            client?.Dispose();
            cache?.Dispose();
        }

        private void CacheBlock(string key, ArchivedMinuteBlock block, long compressedBytes)
        {
            // Each entry is billed at (compressed payload size + per-entry overhead).
            // Sliding expiration biases the cache to keep blobs that are actually re-used
            // — i.e. when an export hits 60 blobs once they all expire together after
            // cacheTtl idle, but a popular item that is queried every minute stays hot.
            var size = Math.Max(1, compressedBytes + perEntryOverheadBytes);
            var options = new MemoryCacheEntryOptions
            {
                Size = size,
                SlidingExpiration = cacheTtl,
                AbsoluteExpirationRelativeToNow = TimeSpan.FromHours(1),
                Priority = CacheItemPriority.Normal,
            };
            cache.Set(key, block, options);
        }

        private static int ConfigInt(IConfiguration config, string key, int fallback)
        {
            return int.TryParse(config[key], out var v) ? v : fallback;
        }

        private static long ConfigLong(IConfiguration config, string key, long fallback)
        {
            return long.TryParse(config[key], out var v) ? v : fallback;
        }
    }
}
