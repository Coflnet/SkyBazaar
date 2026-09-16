using System.Diagnostics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Coflnet.Sky.Items.Client.Api;
using Coflnet.Sky.Items.Client.Model;
using Coflnet.Sky.SkyBazaar.Models;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Newtonsoft.Json.Linq;
using NUnit.Framework;
using StackExchange.Redis;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class BazaarOrderPublisherTests
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task SnapshotSurvivesItemNameFailuresWithoutProducingPartialFillAlerts(bool namesFail)
    {
        var redis = new Mock<IConnectionMultiplexer>();
        var db = new Mock<IDatabase>();
        var items = new Mock<IItemsApi>();
        redis.Setup(r => r.GetDatabase(-1, null)).Returns(db.Object);
        RedisKey[] keys = null;
        RedisValue[] values = null;
        db.Setup(d => d.ScriptEvaluateAsync(BazaarOrderPublisher.PublishScript, It.IsAny<RedisKey[]>(), It.IsAny<RedisValue[]>(), CommandFlags.None))
            .Callback<string, RedisKey[], RedisValue[], CommandFlags>((script, k, v, flags) => { keys = k; values = v; })
            .ReturnsAsync((RedisResult)null);
        if (namesFail)
            items.Setup(i => i.ItemNamesGetAsync(0, default)).ThrowsAsync(new Exception("Items unavailable"));
        else
            items.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<ItemPreview> { new() { Tag = "WHEAT", Name = "Wheat" } });
        await new BazaarOrderPublisher(redis.Object, items.Object, NullLogger<BazaarOrderPublisher>.Instance)
            .Publish("1", "Ekwav", false, () => new() { new OrderEntry {
                UserId = "1", PlayerName = "Ekwav", ItemId = "WHEAT", Amount = 64, Filled = 16, IsEstimate = true
            } });
        Assert.That(keys.Select(k => (string)k), Is.EqualTo(new[] { "bazaar:orders:v1:1", "bazaar:orders:v1", "bazaar:fills:v1" }));
        Assert.That(values, Has.Length.EqualTo(1));
        var state = JObject.Parse(values[0]);
        Assert.That((long)state["Revision"], Is.GreaterThan(DateTime.UtcNow.AddMinutes(-1).Ticks));
        Assert.That((int)state["Orders"][0]["Filled"], Is.EqualTo(16));
        Assert.That((bool)state["Orders"][0]["IsEstimate"], Is.True);
        Assert.That((string)state["ItemNames"]["WHEAT"], Is.EqualTo(namesFail ? "WHEAT" : "Wheat"));
    }

    [Test]
    public async Task RedisDurablyQueuesConfirmedFillOnceWhileConsumerIsOffline()
    {
        var endpoint = Environment.GetEnvironmentVariable("BAZAAR_TEST_REDIS");
        if (string.IsNullOrEmpty(endpoint))
            Assert.Ignore("Set BAZAAR_TEST_REDIS to an isolated Redis instance for the queue integration test.");
        using var redis = await ConnectionMultiplexer.ConnectAsync(endpoint);
        var db = redis.GetDatabase();
        var items = new Mock<IItemsApi>();
        items.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<ItemPreview> { new() { Tag = "WHEAT", Name = "Wheat" } });
        var userId = "queue-test-" + Guid.NewGuid().ToString("N");
        var order = new OrderEntry { UserId = userId, PlayerName = "TestPlayer", ItemId = "WHEAT",
            Timestamp = DateTime.UtcNow, Amount = 64, Filled = 64, IsEstimate = true };
        var publisher = new BazaarOrderPublisher(redis, items.Object, NullLogger<BazaarOrderPublisher>.Instance);
        var before = await db.StreamLengthAsync(BazaarOrderPublisher.FillStream);
        var pushed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = await redis.GetSubscriber().SubscribeAsync(RedisChannel.Literal(BazaarOrderPublisher.Channel));
        subscription.OnMessage(m => pushed.TrySetResult(m.Message));
        await publisher.Publish(userId, order.PlayerName, false, () => new() { order });
        Assert.That(await pushed.Task.WaitAsync(TimeSpan.FromSeconds(5)),
            Is.EqualTo((string)await db.StringGetAsync($"{BazaarOrderPublisher.Channel}:{userId}")));
        Assert.That(await db.StreamLengthAsync(BazaarOrderPublisher.FillStream), Is.EqualTo(before), "An estimated 100% fill must not alert");
        Assert.That(await db.KeyTimeToLiveAsync($"{BazaarOrderPublisher.Channel}:{userId}"),
            Is.InRange(TimeSpan.FromMinutes(9), TimeSpan.FromMinutes(10)), "Snapshots can be rebuilt from the authoritative ledger");
        order.IsEstimate = false;
        order.IsExpired = true;
        order.Claimed = 32;
        await publisher.Publish(userId, order.PlayerName, false, () => new() { order });
        Assert.That(await db.StreamLengthAsync(BazaarOrderPublisher.FillStream), Is.EqualTo(before), "Expired orders must not create fill alerts");
        order.IsExpired = false;
        await publisher.Publish(userId, order.PlayerName, false, () => new() { order });
        await new BazaarOrderPublisher(redis, items.Object, NullLogger<BazaarOrderPublisher>.Instance)
            .Publish(userId, order.PlayerName, false, () => new() { order });
        Assert.That(await db.StreamLengthAsync(BazaarOrderPublisher.FillStream), Is.EqualTo(before + 1), "Deduplication survives publisher restart");
        var messages = await db.StreamRangeAsync(BazaarOrderPublisher.FillStream);
        var message = messages.Select(e => JObject.Parse(e.Values.Single(v => v.Name == "message").Value))
            .Single(m => (string)m["User"]["UserId"] == userId);
        Assert.That((bool)message["Setings"]["StoreIfOffline"], Is.True);
        Assert.That((string)message["SourceSubId"], Is.EqualTo("filled"));
        Assert.That(((string)message["Reference"]).Length, Is.EqualTo(32));
        await subscription.UnsubscribeAsync();
    }
    [Test]
    public async Task SlowItemNameLookupCannotDelayFillPublication()
    {
        var redis = new Mock<IConnectionMultiplexer>();
        var db = new Mock<IDatabase>();
        var items = new Mock<IItemsApi>();
        redis.Setup(r => r.GetDatabase(-1, null)).Returns(db.Object);
        var names = new TaskCompletionSource<List<ItemPreview>>();
        items.Setup(i => i.ItemNamesGetAsync(0, default)).Returns(names.Task);
        var publish = new BazaarOrderPublisher(redis.Object, items.Object, NullLogger<BazaarOrderPublisher>.Instance)
            .Publish("1", "Ekwav", false, () => new() { new OrderEntry { ItemId = "WHEAT", Amount = 64, Filled = 1, IsEstimate = true } });
        await publish.WaitAsync(TimeSpan.FromSeconds(2));
        db.Verify(d => d.ScriptEvaluateAsync(BazaarOrderPublisher.PublishScript, It.IsAny<RedisKey[]>(), It.IsAny<RedisValue[]>(), CommandFlags.None), Times.Once);
        names.SetResult(new());
    }

    [Test]
    public async Task RedisFailureRetainsTheFillEvenIfTheOrderIsClaimedBeforeRetry()
    {
        var redis = new Mock<IConnectionMultiplexer>();
        var db = new Mock<IDatabase>();
        redis.Setup(r => r.GetDatabase(-1, null)).Returns(db.Object);
        var available = false;
        RedisValue[] delivered = null;
        db.Setup(d => d.ScriptEvaluateAsync(BazaarOrderPublisher.PublishScript, It.IsAny<RedisKey[]>(), It.IsAny<RedisValue[]>(), CommandFlags.None))
            .Returns<string, RedisKey[], RedisValue[], CommandFlags>((script, keys, values, flags) => {
                if (!available) throw new RedisServerException("Redis restarting");
                delivered = values;
                return Task.FromResult((RedisResult)null);
            });
        var items = new Mock<IItemsApi>();
        items.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<ItemPreview>());
        var publisher = new BazaarOrderPublisher(redis.Object, items.Object, NullLogger<BazaarOrderPublisher>.Instance);
        await publisher.Publish("1", "Ekwav", false, () => new() { new OrderEntry {
            UserId = "1", PlayerName = "Ekwav", ItemId = "WHEAT", Amount = 64, Filled = 64, IsEstimate = false,
            Timestamp = DateTime.UtcNow
        } });
        await publisher.Publish("1", "Ekwav", false, () => new());
        available = true;
        await publisher.RetryPending();
        Assert.That(JObject.Parse(delivered[0])["Orders"], Is.Empty);
        Assert.That((string)JObject.Parse(delivered[1])["SourceSubId"], Is.EqualTo("filled"));
        await publisher.RetryPending();
        db.Verify(d => d.ScriptEvaluateAsync(BazaarOrderPublisher.PublishScript, It.IsAny<RedisKey[]>(), It.IsAny<RedisValue[]>(), CommandFlags.None), Times.Exactly(3));
    }

    [Test]
    public async Task RebuiltSnapshotsKeepIncreasingAfterRedisLosesItsCounters()
    {
        var redis = new Mock<IConnectionMultiplexer>();
        redis.Setup(r => r.GetDatabase(-1, null)).Returns(Mock.Of<IDatabase>());
        var publisher = new BazaarOrderPublisher(redis.Object, Mock.Of<IItemsApi>(), NullLogger<BazaarOrderPublisher>.Instance);
        var first = JObject.Parse(await publisher.Publish("1", "Ekwav", false, () => new()));
        var second = JObject.Parse(await publisher.Publish("1", "Ekwav", false, () => new()));
        Assert.That((long)second["Revision"], Is.GreaterThan((long)first["Revision"]));
        var replacement = new BazaarOrderPublisher(redis.Object, Mock.Of<IItemsApi>(), NullLogger<BazaarOrderPublisher>.Instance);
        var restored = JObject.Parse(await replacement.Publish("1", "Ekwav", false, () => new()));
        Assert.That((long)restored["Revision"], Is.GreaterThan((long)second["Revision"]));
    }

    [Test]
    public async Task RetryKeepsOriginalAlertTraceWhileSnapshotAdvancesAndPendingGaugesDrain()
    {
        using var listener = new ActivityListener {
            ShouldListenTo = source => source.Name == BazaarTelemetry.SourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> options) => ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);
        var db = new Mock<IDatabase>();
        var redis = new Mock<IConnectionMultiplexer>();
        redis.Setup(r => r.GetDatabase(-1, null)).Returns(db.Object);
        var available = false;
        RedisValue[] delivered = null;
        db.Setup(d => d.ScriptEvaluateAsync(BazaarOrderPublisher.PublishScript, It.IsAny<RedisKey[]>(), It.IsAny<RedisValue[]>(), CommandFlags.None))
            .Returns<string, RedisKey[], RedisValue[], CommandFlags>((script, keys, values, flags) => {
                if (!available) throw new RedisConnectionException(ConnectionFailureType.UnableToConnect, "restarting");
                delivered = values;
                return Task.FromResult(RedisResult.Create((RedisValue)1));
            });
        var items = new Mock<IItemsApi>();
        items.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<ItemPreview>());
        using var publisher = new BazaarOrderPublisher(redis.Object, items.Object, NullLogger<BazaarOrderPublisher>.Instance);
        var pendingUsers = BazaarTelemetry.PendingUsers.Value;
        var pendingFills = BazaarTelemetry.PendingFills.Value;
        JObject first;
        using (var origin = new Activity("observation").SetIdFormat(ActivityIdFormat.W3C).Start())
            first = JObject.Parse(await publisher.Publish("1", "Ekwav", false, () => new() { new() {
                UserId = "1", ItemId = "WHEAT", Amount = 64, Filled = 64, IsEstimate = false, Timestamp = DateTime.UtcNow
            } }));
        await publisher.Publish("1", "Ekwav", false, () => new());
        Assert.That(BazaarTelemetry.PendingUsers.Value, Is.EqualTo(pendingUsers + 1));
        Assert.That(BazaarTelemetry.PendingFills.Value, Is.EqualTo(pendingFills + 1));
        available = true;
        await publisher.RetryPending();
        var alert = JObject.Parse(delivered[1]);
        Assert.That((string)alert["TraceParent"], Is.EqualTo((string)first["TraceParent"]));
        Assert.That((long)alert["Revision"], Is.EqualTo((long)first["Revision"]));
        Assert.That(ActivityContext.TryParse((string)alert["TraceParent"], null, out _), Is.True);
        Assert.That((long)JObject.Parse(delivered[0])["Revision"], Is.GreaterThan((long)alert["Revision"]));
        Assert.That(BazaarTelemetry.PendingUsers.Value, Is.EqualTo(pendingUsers));
        Assert.That(BazaarTelemetry.PendingFills.Value, Is.EqualTo(pendingFills));
    }

}
