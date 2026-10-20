using Amazon;
using Coflnet.Sky.SkyAuctionTracker.Services;
using NUnit.Framework;

namespace Coflnet.Sky.SkyAuctionTracker.Tests
{
    /// <summary>
    /// Regression tests for the S3 client configuration used for Cloudflare R2.
    /// </summary>
    public class S3BlobHistoryStoreConfigTests
    {
        private const string Endpoint = "https://account.eu.r2.cloudflarestorage.com";

        [Test]
        public void BuildConfig_KeepsCustomEndpoint_WhenRegionIsAlsoSet()
        {
            // Assigning RegionEndpoint after ServiceURL silently cleared the ServiceURL
            // inside the SDK, so production resolved s3.auto.amazonaws.com instead of R2.
            var config = S3BlobHistoryStore.BuildConfig(Endpoint, "auto", pathStyle: true);

            Assert.That(config.ServiceURL?.TrimEnd('/'), Is.EqualTo(Endpoint));
            Assert.That(config.AuthenticationRegion, Is.EqualTo("auto"));
            Assert.That(config.RegionEndpoint, Is.Null);
            Assert.That(config.ForcePathStyle, Is.True);
            Assert.That(config.DetermineServiceURL(), Does.StartWith(Endpoint));
        }

        [Test]
        public void BuildConfig_UsesRegionEndpoint_WithoutCustomEndpoint()
        {
            var config = S3BlobHistoryStore.BuildConfig(null, "eu-central-1", pathStyle: false);

            Assert.That(config.RegionEndpoint, Is.EqualTo(RegionEndpoint.EUCentral1));
            Assert.That(config.ServiceURL, Is.Null);
            Assert.That(config.ForcePathStyle, Is.False);
        }
    }
}
