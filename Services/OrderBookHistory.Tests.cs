using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Coflnet.Sky.EventBroker.Client.Api;
using Coflnet.Sky.Items.Client.Api;
using Coflnet.Sky.SkyAuctionTracker.Controllers;
using Coflnet.Sky.SkyBazaar.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using NUnit.Framework;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class OrderBookHistoryTests
{
    private class HistoryService : OrderBookService
    {
        public bool Ready = true;
        public override bool IsReady => Ready;
        public List<(string Item, DateTime Since, int PageSize, byte[] State)> Calls = new();
        public Func<(IEnumerable<OrderEntry>, byte[])> Result = () => (new List<OrderEntry>(), null);
        public List<OrderEntry> Inserted = new();
        public Exception Throw;
        protected override Task InsertToDb(OrderEntry order) { Inserted.Add(order.Copy()); return Task.CompletedTask; }
        protected override Task UpdateInDb(OrderEntry order) => Task.CompletedTask;
        protected override Task RemoveFromDb(OrderEntry order) => Task.CompletedTask;
        protected override Task PublishOrders(OrderEntry changed, bool created = false) => Task.CompletedTask;
        public HistoryService() : base(Mock.Of<ISessionContainer>(), Mock.Of<IMessageApi>(), Mock.Of<IItemsApi>(), NullLogger<OrderBookService>.Instance, null) { }
        protected override Task<(IEnumerable<OrderEntry> Rows, byte[] PagingState)> FetchHistoryPageAsync(string itemTag, DateTime since, int pageSize, byte[] pagingState)
        {
            Calls.Add((itemTag, since, pageSize, pagingState));
            if (Throw != null) throw Throw;
            return Task.FromResult(Result());
        }
    }

    private HistoryService service;
    private OrderBookController controller;

    [SetUp]
    public void Setup()
    {
        service = new HistoryService();
        controller = new OrderBookController(service);
    }

    private static string Json(IActionResult result)
    {
        var json = (JsonResult)result;
        return JsonSerializer.Serialize(json.Value, (JsonSerializerOptions)json.SerializerSettings);
    }

    [Test]
    public async Task HistoryIncludesClosedExpiredAndClaimedOrdersButNeverPlayerNames()
    {
        var placed = DateTime.UtcNow.AddDays(-2);
        service.Result = () => (new[]
        {
            new OrderEntry { ItemId = "WHEAT", UserId = "u1", PlayerName = "SecretPlayerOne", IsSell = false, PricePerUnit = 4.5, Amount = 100, Filled = 100, Claimed = 100, Timestamp = placed, ClosedAt = placed.AddHours(1), FilledAt = placed.AddMinutes(10), IsEstimate = false },
            new OrderEntry { ItemId = "WHEAT", UserId = "u2", PlayerName = "SecretPlayerTwo", IsSell = true, PricePerUnit = 5, Amount = 50, Filled = 10, IsExpired = true, Timestamp = placed.AddHours(1), ClosedAt = placed.AddDays(1) },
            new OrderEntry { ItemId = "WHEAT", UserId = "u3", PlayerName = "SecretPlayerThree", IsSell = true, PricePerUnit = 6, Amount = 5, Timestamp = placed.AddHours(2) },
        }, null);

        var result = await controller.GetHistory("WHEAT", null);
        var json = Json(result);

        Assert.That(json, Does.Not.Contain("Secret").And.Not.Contain("playerName"));
        using var doc = JsonDocument.Parse(json);
        var items = doc.RootElement.GetProperty("items");
        Assert.That(items.GetArrayLength(), Is.EqualTo(3));
        Assert.That(items[0].GetProperty("closedAt").ValueKind, Is.EqualTo(JsonValueKind.String));
        Assert.That(items[0].GetProperty("claimed").GetInt32(), Is.EqualTo(100));
        Assert.That(items[0].GetProperty("filledAt").ValueKind, Is.EqualTo(JsonValueKind.String));
        Assert.That(items[1].GetProperty("isExpired").GetBoolean(), Is.True);
        Assert.That(items[1].GetProperty("isSell").GetBoolean(), Is.True);
        Assert.That(items[2].GetProperty("isSell").GetBoolean(), Is.True);
        Assert.That(items[0].GetProperty("isSell").GetBoolean(), Is.False, "false booleans are written, not omitted");
        Assert.That(items[0].EnumerateObject().Select(p => p.Name), Is.EquivalentTo(new[]
            { "timestamp", "userId", "isSell", "pricePerUnit", "amount", "filled", "claimed", "isExpired", "closedAt", "isEstimate", "filledAt" }));
        Assert.That(doc.RootElement.GetProperty("nextPagingState").ValueKind, Is.EqualTo(JsonValueKind.Null));
    }

    [Test]
    public async Task SinceDefaultsToSevenDaysAndPagingStateRoundTrips()
    {
        service.Result = () => (new List<OrderEntry>(), new byte[] { 1, 250, 255, 62 });
        var json = Json(await controller.GetHistory("WHEAT", null, pageSize: 5000));
        Assert.That(service.Calls.Single().Since, Is.EqualTo(DateTime.UtcNow.AddDays(-7)).Within(TimeSpan.FromMinutes(1)));
        Assert.That(service.Calls.Single().PageSize, Is.EqualTo(5000));
        Assert.That(service.Calls.Single().Item, Is.EqualTo("WHEAT"));
        var cursor = JsonDocument.Parse(json).RootElement.GetProperty("nextPagingState").GetString();
        Assert.That(cursor, Does.Not.Contain("+").And.Not.Contain("/"), "url safe");

        await controller.GetHistory("WHEAT", null, 100, cursor);
        Assert.That(service.Calls[1].State, Is.EqualTo(new byte[] { 1, 250, 255, 62 }));
    }

    [Test]
    public async Task SinceOlderThanEightDaysIsRejectedButWithinIsAccepted()
    {
        Assert.That(await controller.GetHistory("WHEAT", DateTime.UtcNow.AddDays(-8).AddMinutes(-1)), Is.TypeOf<BadRequestObjectResult>());
        Assert.That(await controller.GetHistory("WHEAT", DateTime.UtcNow.AddDays(-9)), Is.TypeOf<BadRequestObjectResult>());
        Assert.That(await controller.GetHistory("WHEAT", DateTime.UtcNow.AddDays(-8).AddMinutes(5)), Is.TypeOf<JsonResult>());
        Assert.That(await controller.GetHistory("WHEAT", DateTime.UtcNow.AddHours(-1)), Is.TypeOf<JsonResult>());
        Assert.That(service.Calls, Has.Count.EqualTo(2), "rejected requests never reach the database");
    }

    [TestCase(0)]
    [TestCase(5001)]
    public async Task PageSizeOutsideOneToFiveThousandIsRejected(int pageSize)
    {
        Assert.That(await controller.GetHistory("WHEAT", null, pageSize), Is.TypeOf<BadRequestObjectResult>());
        Assert.That(service.Calls, Is.Empty);
    }

    [Test]
    public async Task GarbagePagingStateIsRejected()
    {
        Assert.That(await controller.GetHistory("WHEAT", null, 10, "!!not base64!!"), Is.TypeOf<BadRequestObjectResult>());
    }

    [Test]
    public async Task HistoryWaitsForTheLedgerToLoad()
    {
        service.Ready = false;
        controller.ControllerContext = new ControllerContext { HttpContext = new Microsoft.AspNetCore.Http.DefaultHttpContext() };
        var result = await controller.GetHistory("WHEAT", null);
        Assert.That(((ObjectResult)result).StatusCode, Is.EqualTo(503));
        Assert.That(service.Calls, Is.Empty);
    }

    // ---- first fully filled timestamp ----

    [Test]
    public void FilledAtIsStampedOnlyOnTheTransitionToFullyFilled()
    {
        var t1 = new DateTime(2026, 9, 1, 12, 0, 0, DateTimeKind.Utc);
        var order = new OrderEntry { Amount = 10, Filled = 4 };
        Assert.That(OrderBookService.StampFill(order, 4, t1), Is.False);
        order.Filled = 10;
        Assert.That(OrderBookService.StampFill(order, 10, t1), Is.False, "already filled before: legacy, never stamped late");
        Assert.That(order.FilledAt, Is.Null);
        Assert.That(OrderBookService.StampFill(order, 4, t1), Is.True);
        Assert.That(OrderBookService.StampFill(order, 4, t1.AddMinutes(5)), Is.False);
        Assert.That(order.FilledAt, Is.EqualTo(t1), "the first time wins");
    }

    [Test]
    public async Task ReObservingAFilledOrderKeepsFilledAtAndIgnoresClientValues()
    {
        var placed = DateTime.UtcNow.AddHours(-1);
        OrderEntry New(int filled, DateTime? clientFilledAt = null) => new() { ItemId = "WHEAT", UserId = "u", Amount = 10, Filled = filled, PricePerUnit = 5, Timestamp = placed, FilledAt = clientFilledAt };
        await service.AddOrder(New(3, DateTime.UtcNow.AddYears(-1)), observed: true);
        Assert.That(service.Inserted.Last().FilledAt, Is.Null, "client supplied filledAt is ignored");
        await service.AddOrder(New(10), observed: true);
        var stamped = service.Inserted.Last().FilledAt;
        Assert.That(stamped, Is.Not.Null.And.EqualTo(DateTime.UtcNow).Within(TimeSpan.FromMinutes(1)));
        await Task.Delay(20);
        await service.AddOrder(New(10, DateTime.UtcNow.AddYears(-1)), observed: true);
        await service.AddOrder(New(10), observed: true);
        Assert.That(service.Inserted.Last().FilledAt, Is.EqualTo(stamped));
    }

    [Test]
    public async Task LegacyFilledRowIsNotStampedWhenPersistedAgain()
    {
        var placed = DateTime.UtcNow.AddHours(-1);
        await service.AddLoadedOrder(new OrderEntry { ItemId = "WHEAT", UserId = "u", Amount = 10, Filled = 10, PricePerUnit = 5, Timestamp = placed, IsEstimate = true });
        await service.AddOrder(new OrderEntry { ItemId = "WHEAT", UserId = "u", Amount = 10, Filled = 10, PricePerUnit = 5, Timestamp = placed }, observed: true);
        Assert.That(service.Inserted.Last().FilledAt, Is.Null);
    }

    [Test]
    public async Task OldOpenRowsAreReportedExpired()
    {
        service.Result = () => (new[] { new OrderEntry { ItemId = "WHEAT", UserId = "u", Amount = 1, PricePerUnit = 1, Timestamp = DateTime.UtcNow.AddDays(-7).AddMinutes(-1) } }, null);
        using var doc = JsonDocument.Parse(Json(await controller.GetHistory("WHEAT", null)));
        Assert.That(doc.RootElement.GetProperty("items")[0].GetProperty("isExpired").GetBoolean(), Is.True);
    }

    [Test]
    public async Task ServerRejectedPagingStateReturns400()
    {
        service.Throw = new Cassandra.InvalidQueryException("bad paging state");
        Assert.That(await controller.GetHistory("WHEAT", null, 10, WebEncoders.Base64UrlEncode(new byte[] { 1, 2, 3 })), Is.TypeOf<BadRequestObjectResult>());
    }

    [Test]
    public async Task FilledAtColumnIsAddedAdditively()
    {
        var session = new Mock<Cassandra.ISession>();
        session.SetupGet(s => s.Keyspace).Returns("bazaar_test");
        session.Setup(s => s.ExecuteAsync(It.IsAny<Cassandra.IStatement>())).ReturnsAsync(new Cassandra.RowSet());
        await Migrations.AddHasBeenNotifiedMigration.EnsureColumns(session.Object, NullLogger.Instance);
        session.Verify(s => s.ExecuteAsync(It.Is<Cassandra.SimpleStatement>(q => q.QueryString == "ALTER TABLE bazaar_test.order_book ADD filled_at timestamp")), Times.Once);
    }
}
