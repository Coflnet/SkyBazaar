using NUnit.Framework;
using Moq;
using Cassandra;
using Coflnet.Sky.EventBroker.Client.Api;
using Coflnet.Sky.Items.Client.Api;
using Coflnet.Sky.SkyBazaar.Models;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Coflnet.Sky.EventBroker.Client.Model;
using System.Linq;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class OrderBookServiceTests
{
    private class NoDbOrderBookService : OrderBookService
    {
        public bool Ready = true;
        public override bool IsReady => Ready;
        public OrderEntry LastOrder { get; private set; }
        public OrderEntry RemovedOrder { get; private set; }
        public OrderEntry UpdatedOrder { get; private set; }
        public NoDbOrderBookService(ISessionContainer service, IMessageApi messageApi, IItemsApi itemsApi, ILogger<OrderBookService> logger) 
            : base(service, messageApi, itemsApi, logger, null)
        {
        }
        public List<(string UserId, int Filled, bool Created)> Published = new();
        public Func<OrderEntry, Task> OnWrite = _ => Task.CompletedTask;
        protected override Task PublishOrders(OrderEntry order, bool created = false)
        {
            lock (Published)
                Published.Add((order.UserId, order.Filled, created));
            return Task.CompletedTask;
        }
        protected override Task InsertToDb(OrderEntry order)
        {
            LastOrder = order;
            return OnWrite(order);
        }
        protected override Task RemoveFromDb(OrderEntry order)
        {
            RemovedOrder = order;
            return Task.CompletedTask;
        }
        protected override Task UpdateInDb(OrderEntry order)
        {
            UpdatedOrder = order;
            return OnWrite(order);
        }
    }
    private NoDbOrderBookService orderBookService;
    Mock<IItemsApi> itemsApiMock;
    Mock<IMessageApi> messageApiMock;

    [SetUp]
    public void Setup()
    {
        var container = new Mock<ISessionContainer>();
        container.SetupGet(c => c.Session).Returns(null as ISession);
        messageApiMock = new Mock<IMessageApi>();
        itemsApiMock = new Mock<IItemsApi>();
        orderBookService = new NoDbOrderBookService(container.Object, messageApiMock.Object, itemsApiMock.Object, NullLogger<OrderBookService>.Instance);
    }

    [TestCase(true, true)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    [TestCase(false, false)]
    public async Task TopPriceChangesPublishAllOwnersWithoutFillChanges(bool sell, bool kafka)
    {
        var time = DateTime.UtcNow.AddSeconds(-8);
        for (var i = 0; i < 2; i++)
            await orderBookService.AddOrder(new() { UserId = "owner" + i, ItemId = "WHEAT",
                Amount = 10, IsSell = sell, PricePerUnit = 10, Timestamp = time.AddTicks(i * 10000), HasBeenNotified = true });
        Assert.That(orderBookService.GetUserOrders("owner0").Single().IsTopOrder, Is.True);
        Assert.That(orderBookService.GetUserOrders("owner1").Single().IsTopOrder, Is.True, "Equal prices share top status");
        var writes = 0;
        orderBookService.OnWrite = _ => { writes++; return Task.CompletedTask; };
        async Task Observe(bool competitor, int seconds)
        {
            var levels = new List<OrderEntry> { new() { PricePerUnit = 10, Amount = 20 } };
            if (competitor)
                levels.Add(new() { PricePerUnit = sell ? 9 : 11, Amount = 1 });
            if (kafka)
                await orderBookService.BazaarPull(new() { Timestamp = time.AddSeconds(seconds), Products = new() {
                    new() { ProductId = "WHEAT",
                        BuySummery = sell ? levels.Select(o => new dev.BuyOrder { PricePerUnit = o.PricePerUnit, Amount = o.Amount }).ToList() : new(),
                        SellSummary = sell ? new() : levels.Select(o => new dev.SellOrder { PricePerUnit = o.PricePerUnit, Amount = o.Amount }).ToList() } } });
            else
                await orderBookService.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = time.AddSeconds(seconds),
                    SellOrders = sell ? levels : null, BuyOrders = sell ? null : levels });
        }
        orderBookService.Published.Clear();
        await Observe(true, 1);
        Assert.That(orderBookService.Published.Select(p => p.UserId), Is.EquivalentTo(new[] { "owner0", "owner1" }));
        Assert.That(orderBookService.GetUserOrders("owner0").Single().IsTopOrder, Is.False);
        orderBookService.Published.Clear();
        await Observe(true, 2);
        Assert.That(orderBookService.Published, Is.Empty, "Unchanged top price must not spam snapshots");
        await Observe(false, 3);
        Assert.That(orderBookService.Published.Select(p => p.UserId), Is.EquivalentTo(new[] { "owner0", "owner1" }));
        foreach (var user in new[] { "owner0", "owner1" })
        {
            var order = orderBookService.GetUserOrders(user).Single();
            Assert.That(order.IsTopOrder, Is.True);
            Assert.That(order.Filled, Is.Zero);
        }
        Assert.That(writes, Is.Zero, "Position-only changes must not write the ledger");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task ChatAndPersonalOrdersRefreshOtherOwnersWhenAddedAndRemoved(bool observed)
    {
        var time = DateTime.UtcNow.AddMinutes(-1);
        await orderBookService.AddOrder(new() { UserId = "old", ItemId = "WHEAT", Amount = 10,
            IsSell = true, PricePerUnit = 10, Timestamp = time });
        orderBookService.Published.Clear();
        await orderBookService.AddOrder(new() { UserId = "new", ItemId = "WHEAT", Amount = 10,
            IsSell = true, PricePerUnit = 9, Timestamp = time.AddSeconds(1) }, observed);
        Assert.That(orderBookService.GetUserOrders("old").Single().IsTopOrder, Is.False);
        Assert.That(orderBookService.Published.Any(p => p.UserId == "old"), Is.True);
        orderBookService.Published.Clear();
        await orderBookService.RemoveOrder("WHEAT", "new", time.AddSeconds(1), publish: !observed);
        Assert.That(orderBookService.GetUserOrders("old").Single().IsTopOrder, Is.True);
        Assert.That(orderBookService.Published.Any(p => p.UserId == "old"), Is.True);
    }

    [Test]
    public async Task InstantBuyPromotesNextPriceAndClearsCompletedTopStatus()
    {
        var time = DateTime.UtcNow.AddSeconds(-3);
        await orderBookService.AddOrder(new() { UserId = "first", ItemId = "WHEAT", Amount = 1,
            IsSell = true, PricePerUnit = 9, Timestamp = time.AddMinutes(-1) });
        await orderBookService.AddOrder(new() { UserId = "second", ItemId = "WHEAT", Amount = 1,
            IsSell = true, PricePerUnit = 10, Timestamp = time });
        Assert.That(orderBookService.GetUserOrders("second").Single().IsTopOrder, Is.False);
        orderBookService.Published.Clear();
        await orderBookService.ObserveInstantBuy(new() { ItemTag = "WHEAT", Amount = 1, Coins = 9, Timestamp = time.AddSeconds(1) });
        Assert.That(orderBookService.GetUserOrders("first").Single().IsTopOrder, Is.Null);
        Assert.That(orderBookService.GetUserOrders("second").Single().IsTopOrder, Is.True);
        Assert.That(orderBookService.Published.Select(p => p.UserId), Is.EquivalentTo(new[] { "first", "second" }));
    }

    [Test]
    public async Task InstantBuyUsesFifoAndPublishesAllOwnersWithoutDoubleCountingNextSnapshot()
    {
        var time = DateTime.UtcNow.AddSeconds(-3);
        await orderBookService.AddLoadedOrder(new() { UserId = "first", ItemId = "GILL_MEMBRANE", Amount = 2,
            IsSell = true, PricePerUnit = 81.9, Timestamp = time.AddMinutes(-2) });
        await orderBookService.AddLoadedOrder(new() { UserId = "second", ItemId = "GILL_MEMBRANE", Amount = 4,
            IsSell = true, PricePerUnit = 81.9, Timestamp = time.AddMinutes(-1) });
        await orderBookService.AddLoadedOrder(new() { UserId = "buyer", ItemId = "GILL_MEMBRANE", Amount = 4,
            PricePerUnit = 80, Timestamp = time.AddMinutes(-1) });
        var buy = new InstantBuyObservation { ItemTag = "GILL_MEMBRANE", Amount = 3, Coins = 245.7, Timestamp = time };
        Assert.That(await orderBookService.ObserveInstantBuy(buy), Is.True);
        Assert.That(orderBookService.GetUserOrders("first").Single().Filled, Is.EqualTo(2));
        Assert.That(orderBookService.GetUserOrders("second").Single().Filled, Is.EqualTo(1));
        Assert.That(orderBookService.GetUserOrders("first").Single().IsEstimate, Is.True);
        Assert.That(orderBookService.GetUserOrders("buyer").Single().Filled, Is.Zero);
        Assert.That(orderBookService.Published.Select(p => p.UserId), Is.EquivalentTo(new[] { "first", "second" }));
        Assert.That(await orderBookService.ObserveInstantBuy(buy), Is.False);
        Assert.That(await orderBookService.UpdateOrderBook(new() { ItemTag = buy.ItemTag, Timestamp = time.AddTicks(-1),
            SellOrders = new() { new() { PricePerUnit = 81.9, Amount = 6 } } }), Is.False);
        await orderBookService.UpdateOrderBook(new() { ItemTag = buy.ItemTag, Timestamp = time.AddSeconds(1),
            SellOrders = new() { new() { PricePerUnit = 81.9, Amount = 3 } } });
        Assert.That(orderBookService.GetUserOrders("second").Single().Filled, Is.EqualTo(1));
        Assert.That(orderBookService.Published, Has.Count.EqualTo(2));
    }

    [TestCase(2, 30, true)]
    [TestCase(3, 50, true)]
    [TestCase(1, 20, false)]
    [TestCase(4, 70, false)]
    public async Task InstantBuyChecksMultiLevelPriceAndAnonymousLiquidity(int amount, double coins, bool applied)
    {
        var time = DateTime.UtcNow.AddSeconds(-1);
        await orderBookService.AddOrder(new() { ItemId = "GILL_MEMBRANE", Amount = 1, IsSell = true,
            PricePerUnit = 10, Timestamp = time.AddMinutes(-3) });
        await orderBookService.AddLoadedOrder(new() { UserId = "seller", ItemId = "GILL_MEMBRANE", Amount = 2,
            IsSell = true, PricePerUnit = 20, Timestamp = time.AddMinutes(-2) });
        await orderBookService.AddLoadedOrder(new() { UserId = "expired", ItemId = "GILL_MEMBRANE", Amount = 100,
            IsSell = true, IsExpired = true, PricePerUnit = 1, Timestamp = time.AddMinutes(-2) });
        Assert.That(await orderBookService.ObserveInstantBuy(new() { ItemTag = "GILL_MEMBRANE", Amount = amount,
            Coins = coins, Timestamp = time }), Is.EqualTo(applied));
        Assert.That(orderBookService.GetUserOrders("seller").Single().Filled, Is.EqualTo(applied ? amount - 1 : 0));
        Assert.That(orderBookService.GetUserOrders("expired").Single().Filled, Is.Zero);
    }

    [TestCase(-11, true)]
    [TestCase(5, true)]
    [TestCase(-1, false)]
    public async Task InstantBuyDropsStaleFutureAndLoadingObservations(int seconds, bool ready)
    {
        await orderBookService.AddLoadedOrder(new() { UserId = "seller", ItemId = "GILL_MEMBRANE", Amount = 2,
            IsSell = true, PricePerUnit = 81.9, Timestamp = DateTime.UtcNow.AddMinutes(-1) });
        orderBookService.Ready = ready;
        Assert.That(await orderBookService.ObserveInstantBuy(new() { ItemTag = "GILL_MEMBRANE", Amount = 1,
            Coins = 81.9, Timestamp = DateTime.UtcNow.AddSeconds(seconds) }), Is.False);
        Assert.That(orderBookService.GetUserOrders("seller").Single().Filled, Is.Zero);
    }

    [Test]
    public async Task TriggerMessageTest()
    {
        itemsApiMock.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<Items.Client.Model.ItemPreview>() { new() { Tag = "test", Name = "test" } });
        var order = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "test",
            PricePerUnit = 10,
            Timestamp = DateTime.UtcNow,
            UserId = "1"
        };
        await orderBookService.AddOrder(order);
        Assert.That(orderBookService.LastOrder, Is.EqualTo(order));
        var undercutOrder = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "test",
            PricePerUnit = 9,
            Timestamp = order.Timestamp.AddMilliseconds(1),
            UserId = "1"
        };
        await orderBookService.AddOrder(undercutOrder);

    messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("1", It.Is<MessageContainer>(m => m.Message != null && System.Text.RegularExpressions.Regex.Replace(m.Message, "§.", "").Contains("Your sell-order for 1x test has been undercut")), 0, default), Times.Once);
    }

    [Test]
    public async Task TriggerMessageFromPull()
    {
        itemsApiMock.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<Items.Client.Model.ItemPreview>() { new() { Tag = "test", Name = "test" } });
        var order = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "test",
            PricePerUnit = 10,
            Timestamp = DateTime.UtcNow,
            UserId = "1"
        };
        await orderBookService.AddOrder(order);
        await orderBookService.BazaarPull(new dev.BazaarPull()
        {
            Timestamp = DateTime.UtcNow,
            Products = new(){
                new (){
                    ProductId = order.ItemId,
                    SellSummary = new (){},
                    BuySummery = new (){
                        new (){
                            Amount = 1,
                            PricePerUnit = 9
                        }
                    }
                }
            }
        });

    messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("1", It.Is<MessageContainer>(m => m.Message != null && System.Text.RegularExpressions.Regex.Replace(m.Message, "§.", "").Contains("Your sell-order for 1x test has been undercut")), 0, default), Times.Once);
    }

    [Test]
    public async Task OrderRemovedWhenFilled()
    {
        // removed when not present in bazaar pull (only higher price)
        var buyOrder = new OrderEntry()
        {
            Amount = 1,
            IsSell = false,
            ItemId = "test",
            PlayerName = "test",
            PricePerUnit = 10,
            Timestamp = DateTime.UtcNow,
            UserId = "1"
        };
        var sellOrder = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "test",
            PricePerUnit = 11,
            Timestamp = DateTime.UtcNow,
            UserId = "1"
        };
        await orderBookService.AddOrder(buyOrder);
        await orderBookService.AddOrder(sellOrder);
        var orderbook = await orderBookService.GetOrderBook(buyOrder.ItemId);
        Assert.That(orderbook.Buy.Count, Is.EqualTo(1));
        Assert.That(orderbook.Sell.Count, Is.EqualTo(1));
        await orderBookService.BazaarPull(new dev.BazaarPull()
        {
            Timestamp = DateTime.UtcNow,
            Products = new(){
                new (){
                    ProductId = buyOrder.ItemId,
                    SellSummary = new (){
                        new (){
                            Amount = 1,
                            PricePerUnit = 1
                        }
                    },
                    BuySummery = new (){
                        new (){
                            Amount = 1,
                            PricePerUnit = 100
                        }
                    }
                }
            }
        });
        // orders should be removed - new ones present
        orderbook = await orderBookService.GetOrderBook(buyOrder.ItemId);
        Assert.That(orderbook.Buy.Count, Is.EqualTo(1));
        Assert.That(orderbook.Buy.First().PricePerUnit, Is.EqualTo(1));
        Assert.That(orderbook.Sell.Count, Is.EqualTo(1));
        Assert.That(orderbook.Sell.First().PricePerUnit, Is.EqualTo(100));
    }

    [Test]
    public async Task MultipleOutbidsAtOnce()
    {
        // Setup: Add multiple sell orders at different prices
        itemsApiMock.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<Items.Client.Model.ItemPreview>() { new() { Tag = "test", Name = "test" } });
        
        var order1 = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "player1",
            PricePerUnit = 100,
            Timestamp = DateTime.UtcNow,
            UserId = "user1"
        };
        var order2 = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "player2",
            PricePerUnit = 95,
            Timestamp = DateTime.UtcNow,
            UserId = "user2"
        };
        var order3 = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "player3",
            PricePerUnit = 90,
            Timestamp = DateTime.UtcNow,
            UserId = "user3"
        };

        await orderBookService.AddOrder(order1);
        await orderBookService.AddOrder(order2);
        await orderBookService.AddOrder(order3);

        // New order at 85 should outbid all three
        var newOrder = new OrderEntry()
        {
            Amount = 10,
            IsSell = true,
            ItemId = "test",
            PlayerName = "player4",
            PricePerUnit = 85,
            Timestamp = DateTime.UtcNow,
            UserId = "user4"
        };

        await orderBookService.AddOrder(newOrder);

        // All three users should have been notified
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("user1", It.IsAny<MessageContainer>(), 0, default), Times.Once);
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("user2", It.IsAny<MessageContainer>(), 0, default), Times.Once);
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("user3", It.IsAny<MessageContainer>(), 0, default), Times.Once);
        
        // All orders should be marked as notified
        var orderbook = await orderBookService.GetOrderBook("test");
        Assert.That(orderbook.Sell.Where(o => o.UserId != null && o.UserId != newOrder.UserId).All(o => o.HasBeenNotified), Is.True);
    }

    [Test]
    public async Task NoDuplicateNotifications()
    {
        // Setup: Add an order and outbid it
        itemsApiMock.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<Items.Client.Model.ItemPreview>() { new() { Tag = "test", Name = "test" } });
        
        var order1 = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "player1",
            PricePerUnit = 100,
            Timestamp = DateTime.UtcNow,
            UserId = "user1"
        };

        await orderBookService.AddOrder(order1);

        var undercutOrder1 = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "player2",
            PricePerUnit = 90,
            Timestamp = DateTime.UtcNow,
            UserId = "user2"
        };

        await orderBookService.AddOrder(undercutOrder1);

        // First notification should be sent
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("user1", It.IsAny<MessageContainer>(), 0, default), Times.Once);

        // Add another order that would outbid order1 again
        var undercutOrder2 = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "player3",
            PricePerUnit = 85,
            Timestamp = DateTime.UtcNow,
            UserId = "user3"
        };

        await orderBookService.AddOrder(undercutOrder2);

        // user1 should NOT be notified again (still only once)
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("user1", It.IsAny<MessageContainer>(), 0, default), Times.Once);
        // user2 should be notified
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("user2", It.IsAny<MessageContainer>(), 0, default), Times.Once);
    }

    [Test]
    public async Task BuyOrderOutbidMultiple()
    {
        // Test buy orders work the same way (higher price is better)
        itemsApiMock.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<Items.Client.Model.ItemPreview>() { new() { Tag = "test", Name = "test" } });
        
        var order1 = new OrderEntry()
        {
            Amount = 1,
            IsSell = false,
            ItemId = "test",
            PlayerName = "player1",
            PricePerUnit = 50,
            Timestamp = DateTime.UtcNow,
            UserId = "user1"
        };
        var order2 = new OrderEntry()
        {
            Amount = 1,
            IsSell = false,
            ItemId = "test",
            PlayerName = "player2",
            PricePerUnit = 55,
            Timestamp = DateTime.UtcNow,
            UserId = "user2"
        };

        await orderBookService.AddOrder(order1);
        await orderBookService.AddOrder(order2);

        // New order at 60 should outbid both
        var newOrder = new OrderEntry()
        {
            Amount = 10,
            IsSell = false,
            ItemId = "test",
            PlayerName = "player3",
            PricePerUnit = 60,
            Timestamp = DateTime.UtcNow,
            UserId = "user3"
        };

        await orderBookService.AddOrder(newOrder);

        // Both users should have been notified
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("user1", It.IsAny<MessageContainer>(), 0, default), Times.Once);
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("user2", It.IsAny<MessageContainer>(), 0, default), Times.Once);
    }

    [Test]
    public void RestartScenarioOnlyTopOrderNotified()
    {
        // This test simulates a restart where old orders are loaded from DB
        // Only the top order (best price) should be eligible for notifications
        
        var order1 = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "player1",
            PricePerUnit = 100,
            Timestamp = DateTime.UtcNow.AddMinutes(-10),
            UserId = "user1",
            HasBeenNotified = false
        };
        var order2 = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "player2",
            PricePerUnit = 95,
            Timestamp = DateTime.UtcNow.AddMinutes(-5),
            UserId = "user2",
            HasBeenNotified = false
        };
        var order3 = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PlayerName = "player3",
            PricePerUnit = 90, // Best price
            Timestamp = DateTime.UtcNow.AddMinutes(-1),
            UserId = "user3",
            HasBeenNotified = false
        };

        // Simulate loading from DB
        var orderBook = new OrderBook();
        orderBook.Sell.Add(order1);
        orderBook.Sell.Add(order2);
        orderBook.Sell.Add(order3);

        // Mark old orders as notified (simulating restart logic)
        var topSellOrder = orderBook.Sell
            .Where(o => o.UserId != null)
            .OrderBy(o => o.PricePerUnit)
            .FirstOrDefault();
        
        foreach (var order in orderBook.Sell.Where(o => o.UserId != null && o != topSellOrder))
        {
            order.HasBeenNotified = true;
        }

        // Verify only the top order can be notified
        Assert.That(order3.HasBeenNotified, Is.False, "Top order (best price) should not be marked as notified");
        Assert.That(order2.HasBeenNotified, Is.True, "Non-top order should be marked as notified");
        Assert.That(order1.HasBeenNotified, Is.True, "Non-top order should be marked as notified");

        // Now if a new order comes in, only order3 would be notified
        var newOrder = new OrderEntry()
        {
            Amount = 1,
            IsSell = true,
            ItemId = "test",
            PricePerUnit = 85,
            Timestamp = DateTime.UtcNow,
            UserId = "user4"
        };

        var outbidOrders = orderBook.GetAllOutbidOrders(newOrder);
        Assert.That(outbidOrders.Count, Is.EqualTo(1), "Only one order should be eligible for notification");
        Assert.That(outbidOrders[0], Is.EqualTo(order3), "Only the top order should be in the outbid list");
    }

    // Tests for UpdateOrderBook method

    [Test]
    public async Task UpdateOrderBook_BuyOrderOutbid_ShouldNotifyTrackedOrder()
    {
        itemsApiMock.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<Items.Client.Model.ItemPreview>() { new() { Tag = "DIAMOND", Name = "Diamond" } });
        
        // Add initial top buy order with user
        var topBuyOrder = new OrderEntry()
        {
            Amount = 10,
            IsSell = false,
            ItemId = "DIAMOND",
            PricePerUnit = 100,
            Timestamp = DateTime.UtcNow.AddSeconds(-10),
            UserId = "user1",
            PlayerName = "player1"
        };
        
        await orderBookService.AddOrder(topBuyOrder);

        // Update with a higher top buy order, which should outbid the tracked order.
        var update = new OrderBookUpdate()
        {
            ItemTag = "DIAMOND",
            Timestamp = DateTime.UtcNow,
            BuyOrders = new List<OrderEntry>
            {
                new() { Amount = 5, PricePerUnit = 101, IsSell = false },
                new() { Amount = 3, PricePerUnit = 98, IsSell = false }
            }
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.True, "Update should be accepted");
        
        // Verify notification was sent
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("user1", It.Is<MessageContainer>(m => 
            m.Message != null && System.Text.RegularExpressions.Regex.Replace(m.Message, "§.", "").Contains("outbid")), 0, default), Times.Once);
    }

    [Test]
    public async Task UpdateOrderBook_SellOrderUndercut_ShouldNotifyTrackedOrder()
    {
        itemsApiMock.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<Items.Client.Model.ItemPreview>() { new() { Tag = "EMERALD", Name = "Emerald" } });
        
        // Add initial top sell order with user
        var topSellOrder = new OrderEntry()
        {
            Amount = 10,
            IsSell = true,
            ItemId = "EMERALD",
            PricePerUnit = 50,
            Timestamp = DateTime.UtcNow.AddSeconds(-10),
            UserId = "user2",
            PlayerName = "player2"
        };
        
        await orderBookService.AddOrder(topSellOrder);

        // Update with a lower top sell order, which should undercut the tracked order.
        var update = new OrderBookUpdate()
        {
            ItemTag = "EMERALD",
            Timestamp = DateTime.UtcNow,
            SellOrders = new List<OrderEntry>
            {
                new() { Amount = 5, PricePerUnit = 49, IsSell = true },
                new() { Amount = 3, PricePerUnit = 52, IsSell = true }
            }
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.True, "Update should be accepted");
        
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("user2", It.Is<MessageContainer>(m => 
            m.Message != null && System.Text.RegularExpressions.Regex.Replace(m.Message, "§.", "").Contains("Your sell-order for 10x Emerald has been undercut")), 0, default), Times.Once);
    }

    [Test]
    public void Remove_ShouldHandleTrackedOrdersWithoutPlayerName()
    {
        var orderBook = new OrderBook();
        var trackedOrder = new OrderEntry()
        {
            Amount = 20000,
            IsSell = false,
            ItemId = "SEEDS",
            PlayerName = null,
            PricePerUnit = 1.1,
            Timestamp = new DateTime(2026, 4, 8, 15, 53, 11, DateTimeKind.Utc),
            UserId = "7"
        };

        orderBook.Buy.Add(trackedOrder);

        var removed = orderBook.Remove(new OrderEntry()
        {
            Amount = 1,
            IsSell = false,
            ItemId = "SEEDS",
            PlayerName = "Ekwav",
            PricePerUnit = 1.1,
            Timestamp = trackedOrder.Timestamp,
            UserId = "7"
        });

        Assert.That(removed, Is.True);
        Assert.That(orderBook.Buy, Is.Empty);
    }

    [Test]
    public async Task FlipCreatedSellOrder_ShouldUndercutExistingTrackedSellOrders()
    {
        // Simulates the exact flip lifecycle:
        // 1. User A ("11252") has a tracked sell order for SEEDS at 2.9
        // 2. User B ("7" / Ekwav) has a tracked buy order for SEEDS at 1.1
        // 3. User B's buy order flips: buy removed, sell order created at 1.2
        // 4. User A should get an undercut notification
        itemsApiMock.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<Items.Client.Model.ItemPreview>() { new() { Tag = "SEEDS", Name = "Seeds" } });

        // Step 1: User A places a tracked sell order
        var userASellOrder = new OrderEntry()
        {
            Amount = 100,
            IsSell = true,
            ItemId = "SEEDS",
            PlayerName = null,
            PricePerUnit = 2.9,
            Timestamp = DateTime.UtcNow.AddMinutes(-10),
            UserId = "11252"
        };
        await orderBookService.AddOrder(userASellOrder);

        // Step 2: User B places a tracked buy order
        var userBBuyOrder = new OrderEntry()
        {
            Amount = 1024,
            IsSell = false,
            ItemId = "SEEDS",
            PlayerName = null,
            PricePerUnit = 1.1,
            Timestamp = DateTime.UtcNow.AddMinutes(-5),
            UserId = "7"
        };
        await orderBookService.AddOrder(userBBuyOrder);

        // Step 3: Flip detected — SkyUserState removes buy and adds sell
        // (In practice SkyUserState calls RemoveOrder then AddOrder)
        var flipSellOrder = new OrderEntry()
        {
            Amount = 1024,
            IsSell = true,
            ItemId = "SEEDS",
            PlayerName = null,
            PricePerUnit = 1.2,
            Timestamp = DateTime.UtcNow,
            UserId = "7"
        };
        await orderBookService.AddOrder(flipSellOrder);

        // Step 4: User A should be notified their sell order was undercut
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("11252", It.Is<MessageContainer>(mc =>
            mc.Message != null && System.Text.RegularExpressions.Regex.Replace(mc.Message, "§.", "").Contains("Your sell-order for 100x Seeds has been undercut")), 0, default), Times.Once);
    }

    [Test]
    public async Task FlipCreatedSellOrder_NoTrackedSellOrders_NoNotification()
    {
        // When a flip creates a sell order but only anonymous bazaar orders exist,
        // no notification should fire because anonymous orders have no UserId to notify
        itemsApiMock.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<Items.Client.Model.ItemPreview>() { new() { Tag = "SEEDS", Name = "Seeds" } });

        // Anonymous sell orders from bazaar pull
        var anonymousSellOrder = new OrderEntry()
        {
            Amount = 5000,
            IsSell = true,
            ItemId = "SEEDS",
            PlayerName = null,
            PricePerUnit = 2.9,
            Timestamp = DateTime.UtcNow.AddMinutes(-10),
            UserId = null
        };
        await orderBookService.AddOrder(anonymousSellOrder);

        // Flip-created sell order from user 7
        var flipSellOrder = new OrderEntry()
        {
            Amount = 1024,
            IsSell = true,
            ItemId = "SEEDS",
            PlayerName = null,
            PricePerUnit = 1.2,
            Timestamp = DateTime.UtcNow,
            UserId = "7"
        };
        await orderBookService.AddOrder(flipSellOrder);

        // No notifications should be sent (anonymous orders can't be notified)
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync(It.IsAny<string>(), It.IsAny<MessageContainer>(), 0, default), Times.Never);
    }

    [Test]
    public async Task UpdateOrderBook_FutureTimestamp_ShouldIgnore()
    {
        var update = new OrderBookUpdate()
        {
            ItemTag = "DIAMOND",
            Timestamp = DateTime.UtcNow.AddHours(1), // Future timestamp
            BuyOrders = new List<OrderEntry>
            {
                new() { Amount = 5, PricePerUnit = 100, IsSell = false }
            }
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.False, "Update with future timestamp should be ignored");
        
        // Verify no orders were added
        var orderBook = await orderBookService.GetOrderBook("DIAMOND");
        Assert.That(orderBook.Buy.Count, Is.EqualTo(0), "No orders should be added");
    }

    [Test]
    public async Task UpdateOrderBook_NoKafkaTimeYet_AcceptsFreshTimestamp()
    {
        // A fresh observation can seed an empty market book.
        var update = new OrderBookUpdate()
        {
            ItemTag = "FRESH",
            Timestamp = DateTime.UtcNow.AddSeconds(-2),
            BuyOrders = new List<OrderEntry>
            {
                new() { Amount = 5, PricePerUnit = 10, IsSell = false }
            }
        };
        
        var result = await orderBookService.UpdateOrderBook(update);
        Assert.That(result, Is.True, "Update should be accepted when no Kafka time is established yet");
    }

    [Test]
    public async Task UpdateOrderBook_NoUserOrder_NoNotification()
    {
        itemsApiMock.Setup(i => i.ItemNamesGetAsync(0, default)).ReturnsAsync(new List<Items.Client.Model.ItemPreview>() { new() { Tag = "IRON", Name = "Iron" } });
        
        // Add top buy order without user (Kafka data)
        var topBuyOrder = new OrderEntry()
        {
            Amount = 10,
            IsSell = false,
            ItemId = "IRON",
            PricePerUnit = 100,
            Timestamp = DateTime.UtcNow.AddSeconds(-10),
            UserId = null, // No user = Kafka order
            PlayerName = null
        };
        
        await orderBookService.AddOrder(topBuyOrder);

        // Update with lower price
        var update = new OrderBookUpdate()
        {
            ItemTag = "IRON",
            Timestamp = DateTime.UtcNow,
            BuyOrders = new List<OrderEntry>
            {
                new() { Amount = 5, PricePerUnit = 99, IsSell = false }
            }
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.True, "Update should be accepted");
        
        // Verify no notification was sent (no user to notify)
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync(It.IsAny<string>(), It.IsAny<MessageContainer>(), It.IsAny<int>(), default), Times.Never);
    }

    [Test]
    public async Task UpdateOrderBook_PartialUpdate_OnlyBuyOrders()
    {
        // Add both buy and sell orders
        var buyOrder = new OrderEntry()
        {
            Amount = 10,
            IsSell = false,
            ItemId = "STONE",
            PricePerUnit = 100,
            Timestamp = DateTime.UtcNow.AddSeconds(-10),
            UserId = "user1"
        };
        
        var sellOrder = new OrderEntry()
        {
            Amount = 10,
            IsSell = true,
            ItemId = "STONE",
            PricePerUnit = 101,
            Timestamp = DateTime.UtcNow.AddSeconds(-10),
            UserId = "user2"
        };
        
        await orderBookService.AddOrder(buyOrder);
        await orderBookService.AddOrder(sellOrder);

        // Update only buy orders
        var update = new OrderBookUpdate()
        {
            ItemTag = "STONE",
            Timestamp = DateTime.UtcNow,
            BuyOrders = new List<OrderEntry>
            {
                new() { Amount = 5, PricePerUnit = 105, IsSell = false }
            },
            SellOrders = null // Only update buy orders
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.True, "Update should be accepted");
        
        var orderBook = await orderBookService.GetOrderBook("STONE");
        Assert.That(orderBook.Sell.Count, Is.EqualTo(1), "Sell orders should remain unchanged");
        Assert.That(orderBook.Buy.Count, Is.GreaterThan(0), "Buy orders should be updated");
    }

    [Test]
    public async Task UpdateOrderBook_UpdateExistingPriceLevel_ShouldUpdateAmount()
    {
        // Add order at price level 100
        var order = new OrderEntry()
        {
            Amount = 10,
            IsSell = false,
            ItemId = "COAL",
            PricePerUnit = 100,
            Timestamp = DateTime.UtcNow.AddSeconds(-10),
            UserId = null
        };
        
        await orderBookService.AddOrder(order);

        var orderBook = await orderBookService.GetOrderBook("COAL");
        Assert.That(orderBook.Buy.Count, Is.EqualTo(1), "Initial order should be added");
        Assert.That(orderBook.Buy[0].Amount, Is.EqualTo(10), "Initial amount should be 10");

        // Update same price level with different amount
        var update = new OrderBookUpdate()
        {
            ItemTag = "COAL",
            Timestamp = DateTime.UtcNow,
            BuyOrders = new List<OrderEntry>
            {
                new() { Amount = 15, PricePerUnit = 100, IsSell = false } // Same price, different amount
            }
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.True, "Update should be accepted");
        
        orderBook = await orderBookService.GetOrderBook("COAL");
        Assert.That(orderBook.Buy.Sum(o => o.Amount - o.Filled), Is.EqualTo(15));
        Assert.That(orderBook.Buy[0].Amount, Is.EqualTo(10), "Older liquidity keeps its FIFO position");
    }

    [Test]
    public async Task UpdateOrderBook_MultipleOrdersPreserveNonTop_BuyOrders()
    {
        // Add multiple buy orders at different price levels
        await orderBookService.AddOrder(new OrderEntry() { Amount = 5, IsSell = false, ItemId = "OAK", PricePerUnit = 100, Timestamp = DateTime.UtcNow.AddSeconds(-10), UserId = null });
        await orderBookService.AddOrder(new OrderEntry() { Amount = 5, IsSell = false, ItemId = "OAK", PricePerUnit = 95, Timestamp = DateTime.UtcNow.AddSeconds(-10), UserId = null });
        await orderBookService.AddOrder(new OrderEntry() { Amount = 5, IsSell = false, ItemId = "OAK", PricePerUnit = 90, Timestamp = DateTime.UtcNow.AddSeconds(-10), UserId = null });

        var orderBook = await orderBookService.GetOrderBook("OAK");
        Assert.That(orderBook.Buy.Count, Is.EqualTo(3), "Should have 3 buy orders");

        // Update with only top order
        var update = new OrderBookUpdate()
        {
            ItemTag = "OAK",
            Timestamp = DateTime.UtcNow,
            BuyOrders = new List<OrderEntry>
            {
                new() { Amount = 10, PricePerUnit = 100, IsSell = false } // Top price
            }
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.True, "Update should be accepted");
        
        orderBook = await orderBookService.GetOrderBook("OAK");
        Assert.That(orderBook.Buy.Count, Is.GreaterThanOrEqualTo(3), "Non-top orders should be preserved");
        
        Assert.That(orderBook.Buy.Where(o => o.PricePerUnit == 100).Sum(o => o.Amount - o.Filled), Is.EqualTo(10));
    }

    [Test]
    public async Task UpdateOrderBook_OlderThanKafkaTimestamp_ShouldIgnore()
    {
        var kafkaTime = DateTime.UtcNow.AddSeconds(-5);
        var olderTime = kafkaTime.AddSeconds(-2);
        var newerTime = kafkaTime.AddSeconds(2);
        
        // Directly inject Kafka timestamp via reflection (simulates BazaarPull having set it)
        var lastKafkaField = typeof(OrderBookService).GetField("lastKafkaUpdateTime", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var kafkaDict = (ConcurrentDictionary<string, DateTime>)lastKafkaField?.GetValue(orderBookService)
            ?? new ConcurrentDictionary<string, DateTime>();
        kafkaDict.AddOrUpdate("GOLD", kafkaTime, (k, v) => kafkaTime);

        // Try to update with older timestamp - should be rejected
        var update1 = new OrderBookUpdate()
        {
            ItemTag = "GOLD",
            Timestamp = olderTime,
            BuyOrders = new List<OrderEntry>
            {
                new() { Amount = 5, PricePerUnit = 15, IsSell = false }
            }
        };

        var result1 = await orderBookService.UpdateOrderBook(update1);
        Assert.That(result1, Is.False, "Update older than last Kafka update should be ignored");

        // But newer timestamp should be accepted
        var update2 = new OrderBookUpdate()
        {
            ItemTag = "GOLD",
            Timestamp = newerTime,
            BuyOrders = new List<OrderEntry>
            {
                new() { Amount = 5, PricePerUnit = 15, IsSell = false }
            }
        };

        var result2 = await orderBookService.UpdateOrderBook(update2);
        Assert.That(result2, Is.True, "Update newer than last update should be accepted");
    }

    [Test]
    public async Task UpdateOrderBook_AcceptsNewerTimestampThanKafka()
    {
        var kafkaTime = DateTime.UtcNow.AddSeconds(-5);
        
        // Establish Kafka update time
        await orderBookService.AddOrder(new OrderEntry() { Amount = 1, IsSell = true, ItemId = "TEST_NEWER", PricePerUnit = 10, Timestamp = kafkaTime, UserId = null });

        // Update with newer timestamp
        var update = new OrderBookUpdate()
        {
            ItemTag = "TEST_NEWER",
            Timestamp = DateTime.UtcNow, // Newer than Kafka time
            BuyOrders = new List<OrderEntry>
            {
                new() { Amount = 5, PricePerUnit = 15, IsSell = false }
            }
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.True, "Update with newer timestamp should be accepted");
    }

    [Test]
    public async Task UpdateOrderBook_EmptyOrderLists_ShouldNotThrow()
    {
        var update = new OrderBookUpdate()
        {
            ItemTag = "EMPTY",
            Timestamp = DateTime.UtcNow,
            BuyOrders = new List<OrderEntry>(),
            SellOrders = new List<OrderEntry>()
        };

        Assert.DoesNotThrowAsync(async () => await orderBookService.UpdateOrderBook(update), "Empty order lists should not throw");
    }

    [Test]
    public async Task AddOrder_NegativeAmount_ShouldBeRejected()
    {
        var negativeOrder = new OrderEntry()
        {
            Amount = -1,
            IsSell = false,
            ItemId = "BOOSTER_COOKIE",
            PricePerUnit = 9806978.7,
            Timestamp = DateTime.UtcNow,
            UserId = "user1"
        };

        await orderBookService.AddOrder(negativeOrder);

        var orderBook = await orderBookService.GetOrderBook("BOOSTER_COOKIE");
        Assert.That(orderBook.Buy.Count, Is.EqualTo(0), "Negative amount orders should not be added");
    }

    [Test]
    public async Task UpdateOrderBook_NegativeAmount_ShouldRemoveOrder()
    {
        // Add an initial order
        var initialOrder = new OrderEntry()
        {
            Amount = 10,
            IsSell = false,
            ItemId = "COOKIE",
            PricePerUnit = 100,
            Timestamp = DateTime.UtcNow.AddSeconds(-10),
            UserId = "user1"
        };

        await orderBookService.AddOrder(initialOrder);

        var orderBook = await orderBookService.GetOrderBook("COOKIE");
        Assert.That(orderBook.Buy.Count, Is.EqualTo(1), "Initial order should be added");

        // Update with negative amount (order was filled)
        var update = new OrderBookUpdate()
        {
            ItemTag = "COOKIE",
            Timestamp = DateTime.UtcNow,
            BuyOrders = new List<OrderEntry>
            {
                new() { Amount = -1, PricePerUnit = 100, IsSell = false }
            }
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.True, "Update should be accepted");

        orderBook = await orderBookService.GetOrderBook("COOKIE");
        Assert.That(orderBook.Buy.Count, Is.EqualTo(0), "Order with negative amount should be removed");
    }

    [Test]
    public async Task UpdateOrderBook_ZeroAmount_ShouldRemoveOrder()
    {
        // Add an initial order
        var initialOrder = new OrderEntry()
        {
            Amount = 5,
            IsSell = true,
            ItemId = "DIAMOND",
            PricePerUnit = 50,
            Timestamp = DateTime.UtcNow.AddSeconds(-10),
            UserId = "user2"
        };

        await orderBookService.AddOrder(initialOrder);

        var orderBook = await orderBookService.GetOrderBook("DIAMOND");
        Assert.That(orderBook.Sell.Count, Is.EqualTo(1), "Initial order should be added");

        // Update with zero amount (order was completely filled)
        var update = new OrderBookUpdate()
        {
            ItemTag = "DIAMOND",
            Timestamp = DateTime.UtcNow,
            SellOrders = new List<OrderEntry>
            {
                new() { Amount = 0, PricePerUnit = 50, IsSell = true }
            }
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.True, "Update should be accepted");

        orderBook = await orderBookService.GetOrderBook("DIAMOND");
        Assert.That(orderBook.Sell.Count, Is.EqualTo(0), "Order with zero amount should be removed");
    }

    [Test]
    public async Task UpdateOrderBook_PartialFillScenario_ShouldRemoveFilledOrders()
    {
        // Simulate the DUSTGRAIN scenario: existing order book state
        var existingOrders = new List<OrderEntry>
        {
            new() { Amount = 9, PricePerUnit = 134199.9, IsSell = true, ItemId = "DUSTGRAIN", UserId = null, Timestamp = DateTime.UtcNow.AddSeconds(-30) },
            new() { Amount = 2, PricePerUnit = 134200.0, IsSell = true, ItemId = "DUSTGRAIN", UserId = null, Timestamp = DateTime.UtcNow.AddSeconds(-30) },
            new() { Amount = 34, PricePerUnit = 134281.8, IsSell = true, ItemId = "DUSTGRAIN", UserId = null, Timestamp = DateTime.UtcNow.AddSeconds(-30) },
            new() { Amount = 1, PricePerUnit = 134281.9, IsSell = true, ItemId = "DUSTGRAIN", UserId = null, Timestamp = DateTime.UtcNow.AddSeconds(-30) },
            new() { Amount = 39, PricePerUnit = 134282.0, IsSell = true, ItemId = "DUSTGRAIN", UserId = null, Timestamp = DateTime.UtcNow.AddSeconds(-30) },
            new() { Amount = 5, PricePerUnit = 134282.1, IsSell = true, ItemId = "DUSTGRAIN", UserId = null, Timestamp = DateTime.UtcNow.AddSeconds(-30) },
            new() { Amount = 7, PricePerUnit = 134282.2, IsSell = true, ItemId = "DUSTGRAIN", UserId = null, Timestamp = DateTime.UtcNow.AddSeconds(-30) },
            new() { Amount = 5, PricePerUnit = 134282.3, IsSell = true, ItemId = "DUSTGRAIN", UserId = null, Timestamp = DateTime.UtcNow.AddSeconds(-30) },
            new() { Amount = 2, PricePerUnit = 134282.8, IsSell = true, ItemId = "DUSTGRAIN", UserId = null, Timestamp = DateTime.UtcNow.AddSeconds(-30) },
            new() { Amount = 3, PricePerUnit = 134282.9, IsSell = true, ItemId = "DUSTGRAIN", UserId = null, Timestamp = DateTime.UtcNow.AddSeconds(-30) },
            new() { Amount = 8, PricePerUnit = 134283.0, IsSell = true, ItemId = "DUSTGRAIN", UserId = null, Timestamp = DateTime.UtcNow.AddSeconds(-30) }
        };

        foreach (var order in existingOrders)
        {
            await orderBookService.AddOrder(order);
        }

        var orderBook = await orderBookService.GetOrderBook("DUSTGRAIN");
        Assert.That(orderBook.Sell.Count, Is.EqualTo(11), "Should have 11 existing sell orders");

        // Post update with partial fill - some orders completely removed (filled), amounts reduced for others
        var update = new OrderBookUpdate()
        {
            ItemTag = "DUSTGRAIN",
            Timestamp = DateTime.UtcNow,
            SellOrders = new List<OrderEntry>
            {
                new() { Amount = 20, PricePerUnit = 134281.8, IsSell = true }, // Was 34, now 20 (14 filled)
                new() { Amount = 1, PricePerUnit = 134281.9, IsSell = true },  // Same (0 filled)
                // 134282.0 missing - completely filled
                new() { Amount = 5, PricePerUnit = 134282.1, IsSell = true },  // Same (0 filled)
                new() { Amount = 7, PricePerUnit = 134282.2, IsSell = true },  // Same (0 filled)
                new() { Amount = 5, PricePerUnit = 134282.3, IsSell = true },  // Same (0 filled)
                new() { Amount = 2, PricePerUnit = 134282.8, IsSell = true },  // Same (0 filled)
                new() { Amount = 3, PricePerUnit = 134282.9, IsSell = true }   // Same (0 filled)
                // 134283.0 missing - completely filled
            }
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.True, "Update should be accepted");

        orderBook = await orderBookService.GetOrderBook("DUSTGRAIN");
        
        // Lower sell prices have been passed by the market; a missing level inside the
        // displayed range also vanishes. The worse price outside the visible range remains.
        Assert.That(orderBook.Sell.Count, Is.EqualTo(8), "Price moved past two lower levels, and one level vanished within the visible range");
        
        // Verify the correct order was removed
        var pricesAfterUpdate = orderBook.Sell.OrderBy(o => o.PricePerUnit).Select(o => o.PricePerUnit).ToList();
        Assert.That(pricesAfterUpdate, Does.Not.Contain(134199.9), "The market moved past this sell price");
        Assert.That(pricesAfterUpdate, Does.Not.Contain(134200.0), "The market moved past this sell price");
        Assert.That(pricesAfterUpdate, Does.Not.Contain(134282.0), "134282.0 should be removed (not in update, within range)");
        Assert.That(pricesAfterUpdate, Contains.Item(134283.0), "Orders above update range should be preserved");
    }
    [Test]
    public async Task PartialFillsFollowFifoAndPublishOnlyAffectedUsers()
    {
        var start = DateTime.UtcNow.AddSeconds(-15);
        await orderBookService.AddOrder(new() { ItemId = "WHEAT", Amount = 10, PricePerUnit = 5, Timestamp = start });
        var first = new OrderEntry { UserId = "1", PlayerName = "one", ItemId = "WHEAT", Amount = 20,
            PricePerUnit = 5, Timestamp = start.AddSeconds(1) };
        var second = new OrderEntry { UserId = "2", PlayerName = "two", ItemId = "WHEAT", Amount = 30,
            PricePerUnit = 5, Timestamp = start.AddSeconds(2) };
        await orderBookService.AddOrder(first);
        await orderBookService.AddOrder(second);
        orderBookService.Published.Clear();
        await orderBookService.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = start.AddSeconds(10),
            BuyOrders = new() { new() { PricePerUnit = 5, Amount = 35 } } });
        Assert.That(first.Amount, Is.EqualTo(20));
        Assert.That(first.Filled, Is.EqualTo(15));
        Assert.That(second.Filled, Is.Zero);
        Assert.That(orderBookService.Published, Is.EqualTo(new[] { ("1", 15, false) }));
        await orderBookService.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = start.AddSeconds(11),
            BuyOrders = new() { new() { PricePerUnit = 5, Amount = 25 } } });
        Assert.That(first.Filled, Is.EqualTo(20));
        Assert.That(second.Filled, Is.EqualTo(5));
        Assert.That(orderBookService.GetUserOrders("1").Single().Filled, Is.EqualTo(20), "Keep filled orders until claimed");
        await orderBookService.RemoveOrder("WHEAT", "1", first.Timestamp);
        Assert.That(orderBookService.GetUserOrders("1"), Is.Empty);
        Assert.That(orderBookService.GetUserOrders("2"), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task KafkaShrinkingLevelAdvancesTrackedFillWithoutOrderMenu()
    {
        var order = new OrderEntry { UserId = "1", ItemId = "WHEAT", Amount = 64,
            IsSell = true, PricePerUnit = 10, Timestamp = DateTime.UtcNow.AddSeconds(-10) };
        await orderBookService.AddOrder(order);
        await orderBookService.BazaarPull(new dev.BazaarPull { Timestamp = DateTime.UtcNow,
            Products = new() { new() { ProductId = "WHEAT", BuySummery = new() {
                new() { PricePerUnit = 10, Amount = 48 } }, SellSummary = new() } } });
        Assert.That(order.Filled, Is.EqualTo(16));
        Assert.That(orderBookService.Published.Last(), Is.EqualTo(("1", 16, false)));
    }

    [Test]
    public async Task LateRegistrationDoesNotDoubleCountAndReplayDoesNotUndoFills()
    {
        var time = DateTime.UtcNow.AddSeconds(-10);
        await orderBookService.AddOrder(new() { ItemId = "WHEAT", Amount = 64, PricePerUnit = 10, Timestamp = time });
        await orderBookService.AddOrder(new() { UserId = "1", ItemId = "WHEAT", Amount = 32,
            PricePerUnit = 10, Timestamp = time.AddSeconds(-1) });
        var book = await orderBookService.GetOrderBook("WHEAT");
        Assert.That(book.Buy.Sum(o => o.Amount - o.Filled), Is.EqualTo(64));
        await orderBookService.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = time.AddSeconds(1),
            BuyOrders = new() { new() { Amount = 48, PricePerUnit = 10 } } });
        await orderBookService.AddOrder(new() { UserId = "1", ItemId = "WHEAT", Amount = 32,
            PricePerUnit = 10, Timestamp = time.AddSeconds(-1) });
        Assert.That(orderBookService.GetUserOrders("1").Single().Filled, Is.EqualTo(16));
        Assert.That((await orderBookService.GetOrderBook("WHEAT")).Buy.Sum(o => o.Amount - o.Filled), Is.EqualTo(48));
    }

    [Test]
    public async Task ObservationsCorrectFillsAndRemoveOnlyTheObservedPlayersOrders()
    {
        var time = DateTime.UtcNow.AddMinutes(-1);
        await orderBookService.AddOrder(new() { UserId = "1", PlayerName = "one", ItemId = "WHEAT", Amount = 64,
            Filled = 30, PricePerUnit = 10, Timestamp = time });
        await orderBookService.AddOrder(new() { UserId = "1", PlayerName = "alt", ItemId = "WHEAT", Amount = 64,
            PricePerUnit = 10, Timestamp = time.AddSeconds(1) });
        var observation = new PlayerOrderObservation { UserId = "1", PlayerName = "one", Timestamp = time.AddSeconds(5),
            Orders = new() { new() { ItemId = "WHEAT", Amount = 64, Filled = 12, PricePerUnit = 10, Timestamp = time } } };
        await orderBookService.ObservePlayerOrders(observation);
        Assert.That(orderBookService.GetUserOrders("1").Single(o => o.PlayerName == "one").Filled, Is.EqualTo(12));
        await orderBookService.ObservePlayerOrders(new() { UserId = "1", PlayerName = "one", Timestamp = time.AddSeconds(6) });
        await orderBookService.ObservePlayerOrders(observation);
        Assert.That(orderBookService.GetUserOrders("1").Single().PlayerName, Is.EqualTo("alt"));
    }
    [Test]
    public async Task StartupLoadIdentifiesExistingLiquidityAndCannotReviveCancelledOrders()
    {
        var time = DateTime.UtcNow.AddMinutes(-1);
        await orderBookService.AddOrder(new() { ItemId = "WHEAT", Amount = 100, PricePerUnit = 10, Timestamp = time.AddSeconds(5) });
        var saved = new OrderEntry { UserId = "1", ItemId = "WHEAT", Amount = 40, Filled = 10,
            PricePerUnit = 10, Timestamp = time };
        await orderBookService.AddLoadedOrder(saved);
        Assert.That((await orderBookService.GetOrderBook("WHEAT")).Buy.Sum(o => o.Amount - o.Filled), Is.EqualTo(100));
        await orderBookService.RemoveOrder("WHEAT", "1", saved.Timestamp);
        await orderBookService.AddLoadedOrder(saved);
        Assert.That(orderBookService.GetUserOrders("1"), Is.Empty);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task CompletionStaysEstimatedUntilMarketMovesPastItsPrice(bool sell)
    {
        var time = DateTime.UtcNow.AddSeconds(-5);
        await orderBookService.AddOrder(new() { UserId = "1", PlayerName = "Ekwav", ItemId = "WHEAT",
            Amount = 64, PricePerUnit = 10, IsSell = sell, Timestamp = time });
        async Task Update(int seconds, double price, int amount)
        {
            var levels = new List<OrderEntry> { new() { PricePerUnit = price, Amount = amount } };
            await orderBookService.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = time.AddSeconds(seconds),
                BuyOrders = sell ? null : levels, SellOrders = sell ? levels : null });
        }
        await Update(1, 10, 32);
        Assert.That(orderBookService.GetUserOrders("1").Single().IsEstimate, Is.True);
        await Update(2, 10, 0);
        Assert.That(orderBookService.GetUserOrders("1").Single().Filled, Is.EqualTo(64));
        Assert.That(orderBookService.GetUserOrders("1").Single().IsEstimate, Is.True);
        await Update(3, sell ? 9 : 11, 10); // Being undercut/outbid is not confirmation.
        Assert.That(orderBookService.GetUserOrders("1").Single().IsEstimate, Is.True);
        await Update(4, sell ? 11 : 9, 10);
        Assert.That(orderBookService.GetUserOrders("1").Single().IsEstimate, Is.False);
        Assert.That(orderBookService.GetUserOrders("1").Single().Filled, Is.EqualTo(64));
    }

    [Test]
    public async Task DelayedPlayerStateViewCannotUndoNewerDirectPriceEstimate()
    {
        var time = DateTime.UtcNow.AddSeconds(-15);
        var order = new OrderEntry { UserId = "1", PlayerName = "Ekwav", ItemId = "WHEAT", Amount = 64,
            PricePerUnit = 10, Timestamp = time };
        await orderBookService.AddOrder(order);
        await orderBookService.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = time.AddSeconds(10),
            BuyOrders = new() { new() { PricePerUnit = 10, Amount = 32 } } });
        var oldView = new PlayerOrderObservation { UserId = "1", PlayerName = "Ekwav", Timestamp = time.AddSeconds(5),
            Orders = new() { new() { ItemId = "WHEAT", Amount = 64, Filled = 16, PricePerUnit = 10, Timestamp = time } } };
        await orderBookService.ObservePlayerOrders(oldView);
        Assert.That(orderBookService.GetUserOrders("1").Single().Filled, Is.EqualTo(32));
        Assert.That(orderBookService.GetUserOrders("1").Single().IsEstimate, Is.True);
        oldView.Timestamp = time.AddSeconds(11);
        oldView.Orders[0].Filled = 40;
        await orderBookService.ObservePlayerOrders(oldView);
        Assert.That(orderBookService.GetUserOrders("1").Single().Filled, Is.EqualTo(40));
        Assert.That(orderBookService.GetUserOrders("1").Single().IsEstimate, Is.False);
    }

    [Test]
    public async Task UnchangedMarketTickCannotBlockPersonalCorrection()
    {
        var time = DateTime.UtcNow.AddSeconds(-8);
        await orderBookService.AddOrder(new() { UserId = "1", PlayerName = "Ekwav", ItemId = "WHEAT",
            Amount = 64, PricePerUnit = 10, Timestamp = time });
        await orderBookService.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = time.AddSeconds(1),
            BuyOrders = new() { new() { PricePerUnit = 10, Amount = 32 } } });
        await orderBookService.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = time.AddSeconds(5),
            BuyOrders = new() { new() { PricePerUnit = 10, Amount = 32 } } });
        await orderBookService.ObservePlayerOrders(new() { UserId = "1", PlayerName = "Ekwav", Timestamp = time.AddSeconds(3),
            Orders = new() { new() { ItemId = "WHEAT", Amount = 64, Filled = 16, PricePerUnit = 10, Timestamp = time } } });
        Assert.That(orderBookService.GetUserOrders("1").Single().Filled, Is.EqualTo(16));
        Assert.That(orderBookService.GetUserOrders("1").Single().IsEstimate, Is.False);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task ExpiredOrdersKeepObservedFillsAndNeverReenterMatching(bool loaded)
    {
        var time = DateTime.UtcNow.AddSeconds(-8);
        var order = new OrderEntry { UserId = "1", PlayerName = "Ekwav", ItemId = "AGATHA_COUPON",
            Amount = 160, Filled = 91, Claimed = 40, PricePerUnit = 9673, IsExpired = loaded, Timestamp = time };
        if (loaded)
            await orderBookService.AddLoadedOrder(order);
        else
        {
            await orderBookService.AddOrder(order);
            await orderBookService.UpdateOrderBook(new() { ItemTag = order.ItemId, Timestamp = time.AddSeconds(3),
                BuyOrders = new() { new() { PricePerUnit = 9000, Amount = 10 } } });
            Assert.That(orderBookService.GetUserOrders("1").Single().Filled, Is.EqualTo(160));
            await orderBookService.ObservePlayerOrders(new() { UserId = "1", PlayerName = "Ekwav", Timestamp = time.AddSeconds(2),
                Orders = new() { new() { ItemId = order.ItemId, Amount = 160, Filled = 91, Claimed = 40,
                    PricePerUnit = 9673, IsExpired = true, Timestamp = time } } });
        }
        await orderBookService.UpdateOrderBook(new() { ItemTag = order.ItemId, Timestamp = time.AddSeconds(4),
            BuyOrders = new() { new() { PricePerUnit = 8000, Amount = 10 } } });
        var saved = orderBookService.GetUserOrders("1").Single();
        Assert.That(saved.Filled, Is.EqualTo(91));
        Assert.That(saved.Claimed, Is.EqualTo(40));
        Assert.That(saved.IsExpired, Is.True);
        Assert.That((await orderBookService.GetOrderBook(order.ItemId)).Buy.All(o => o.UserId == null), Is.True);
    }

    [Test]
    public async Task PartialClaimUpdatesOnlyItsOrderAndRetriedClaimsCannotDecreaseWithdrawals()
    {
        var time = DateTime.UtcNow.AddSeconds(-8);
        await orderBookService.AddOrder(new() { UserId = "1", ItemId = "WHEAT", Amount = 1024,
            Filled = 800, PricePerUnit = 10, Timestamp = time });
        await orderBookService.AddOrder(new() { UserId = "1", ItemId = "OTHER", Amount = 64,
            Filled = 32, PricePerUnit = 10, Timestamp = time });
        foreach (var claimed in new[] { 512, 256 })
            await orderBookService.AddOrder(new() { UserId = "1", ItemId = "WHEAT", Amount = 1024,
                Filled = claimed, Claimed = claimed, PricePerUnit = 10, Timestamp = time });
        var orders = orderBookService.GetUserOrders("1");
        Assert.That(orders.Single(o => o.ItemId == "WHEAT").Claimed, Is.EqualTo(512));
        Assert.That(orders.Single(o => o.ItemId == "WHEAT").Filled, Is.EqualTo(800));
        Assert.That(orders.Single(o => o.ItemId == "OTHER").Filled, Is.EqualTo(32));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task MarketPollKeepsExpiredUnclaimedOrdersUntilPersonalViewRemovesThem(bool loaded)
    {
        var now = DateTime.UtcNow;
        var order = new OrderEntry { UserId = "7", PlayerName = "Ekwav", ItemId = "SHARD_HIDEONWALL",
            IsSell = true, Amount = 129, Filled = 129, Claimed = 64, PricePerUnit = 197783,
            Timestamp = now.AddDays(-12), IsExpired = true, IsEstimate = false };
        if (loaded)
            await orderBookService.AddLoadedOrder(order);
        else
            await orderBookService.ObservePlayerOrders(new() { UserId = "7", PlayerName = "Ekwav",
                Timestamp = now, Orders = new() { order } });

        for (var i = 0; i < 2; i++)
            await orderBookService.BazaarPull(new() { Timestamp = now.AddSeconds(i), Products = new() {
                new() { ProductId = order.ItemId, BuySummery = new(), SellSummary = new() } } });

        var saved = orderBookService.GetUserOrders("7").Single();
        Assert.That(saved.Filled, Is.EqualTo(129));
        Assert.That(saved.Claimed, Is.EqualTo(64));
        Assert.That(saved.IsExpired, Is.True);
        Assert.That(orderBookService.RemovedOrder, Is.Null);
        Assert.That((await orderBookService.GetOrderBook(order.ItemId)).Sell, Is.Empty);
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync(It.IsAny<string>(), It.IsAny<MessageContainer>(), 0, default), Times.Never);

        await orderBookService.ObservePlayerOrders(new() { UserId = "7", PlayerName = "Ekwav",
            Timestamp = now.AddSeconds(3), Orders = new() });
        Assert.That(orderBookService.GetUserOrders("7"), Is.Empty);
    }

    [Test]
    public async Task MarketPollMarksNewlyExpiredOrdersWithoutChangingFillOrClaimState()
    {
        var now = DateTime.UtcNow;
        var order = new OrderEntry { UserId = "7", PlayerName = "Ekwav", ItemId = "WHEAT",
            Amount = 129, Filled = 91, Claimed = 40, PricePerUnit = 10, Timestamp = now.AddDays(-8) };
        await orderBookService.AddOrder(order);
        // Simulate a tracked row whose expiry flag has not yet been updated.
        order.IsExpired = false;
        order.IsEstimate = true;
        OrderEntry persisted = null;
        orderBookService.OnWrite = entry => { persisted = entry.Copy(); return Task.CompletedTask; };
        var publications = orderBookService.Published.Count;
        for (var i = 0; i < 2; i++)
            await orderBookService.BazaarPull(new() { Timestamp = now.AddSeconds(i), Products = new() {
                new() { ProductId = order.ItemId, BuySummery = new(), SellSummary = new() } } });

        Assert.That(order.IsExpired, Is.True);
        Assert.That(order.Filled, Is.EqualTo(91));
        Assert.That(order.Claimed, Is.EqualTo(40));
        Assert.That(order.IsEstimate, Is.True);
        Assert.That(orderBookService.GetUserOrders("7"), Has.Count.EqualTo(1));
        Assert.That(persisted.IsExpired, Is.True);
        Assert.That(orderBookService.Published.Count, Is.EqualTo(publications + 1));
        Assert.That((await orderBookService.GetOrderBook(order.ItemId)).Buy.All(o => o.UserId == null), Is.True);
    }

    [Test]
    public async Task SevenDayExpiryStopsMatchingWithoutAnotherMenuUpload()
    {
        var time = DateTime.UtcNow.AddSeconds(-4);
        await orderBookService.AddOrder(new() { UserId = "1", PlayerName = "Ekwav", ItemId = "WHEAT",
            Amount = 64, Filled = 16, PricePerUnit = 10, Timestamp = time.AddDays(-7).AddSeconds(1) });
        await orderBookService.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = time.AddSeconds(2),
            BuyOrders = new() { new() { PricePerUnit = 9, Amount = 10 } } });
        Assert.That(orderBookService.GetUserOrders("1").Single().Filled, Is.EqualTo(16));
        await orderBookService.AddLoadedOrder(new() { UserId = "2", ItemId = "WHEAT",
            Amount = 64, Filled = 16, PricePerUnit = 10, Timestamp = time.AddDays(-7) });
        Assert.That(orderBookService.GetUserOrders("2").Single().IsExpired, Is.True);
    }

    [Test]
    public async Task PersonalRefreshPublishesOneSnapshotWithoutHistoricalOutbidAlerts()
    {
        var time = DateTime.UtcNow.AddSeconds(-8);
        await orderBookService.ObservePlayerOrders(new() { UserId = "1", PlayerName = "Ekwav", Timestamp = time,
            Orders = Enumerable.Range(0, 20).Select(i => new OrderEntry { ItemId = "WHEAT", Amount = 64,
                PricePerUnit = 10 + i, Timestamp = time.AddMilliseconds(-i) }).ToList() });
        Assert.That(orderBookService.GetUserOrders("1"), Has.Count.EqualTo(20));
        Assert.That(orderBookService.Published, Has.Count.EqualTo(1));
        messageApiMock.VerifyNoOtherCalls();
        itemsApiMock.VerifyNoOtherCalls();
        orderBookService.Published.Clear();
        await orderBookService.ObservePlayerOrders(new() { UserId = "1", PlayerName = "Ekwav", Timestamp = time.AddSeconds(1) });
        Assert.That(orderBookService.GetUserOrders("1"), Is.Empty);
        Assert.That(orderBookService.Published, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task ExpiredFastObservationsAreDroppedEvenForAnEmptyBook()
    {
        var accepted = await orderBookService.UpdateOrderBook(new() { ItemTag = "WHEAT",
            Timestamp = DateTime.UtcNow.AddSeconds(-11), BuyOrders = new() { new() { Amount = 64, PricePerUnit = 10 } } });
        Assert.That(accepted, Is.False);
        Assert.That((await orderBookService.GetOrderBook("WHEAT")).Buy, Is.Empty);
    }

    [Test]
    public async Task InvalidPersonalViewReturnsBadRequestWithoutReplacingTrackedOrders()
    {
        var controller = new Coflnet.Sky.SkyAuctionTracker.Controllers.OrderBookController(orderBookService);
        var result = await controller.ObservePlayerOrders(new() {
            UserId = "1", PlayerName = "Ekwav", Timestamp = DateTime.UtcNow.AddSeconds(-1),
            Orders = new() { new() { Amount = 64, ItemId = null } }
        });
        Assert.That(result, Is.TypeOf<Microsoft.AspNetCore.Mvc.BadRequestObjectResult>());
        Assert.That(orderBookService.LastOrder, Is.Null);
        Assert.That(orderBookService.RemovedOrder, Is.Null);
    }

    [Test]
    public async Task LoadingRejectsPersonalUpdatesWithRetryHintAndDropsFastPrices()
    {
        orderBookService.Ready = false;
        var controller = new Coflnet.Sky.SkyAuctionTracker.Controllers.OrderBookController(orderBookService) {
            ControllerContext = new Microsoft.AspNetCore.Mvc.ControllerContext {
                HttpContext = new Microsoft.AspNetCore.Http.DefaultHttpContext()
            }
        };
        var result = await controller.AddOrder(new() { ItemId = "WHEAT", UserId = "1", Amount = 64 });
        Assert.That(((Microsoft.AspNetCore.Mvc.ObjectResult)result).StatusCode, Is.EqualTo(503));
        Assert.That(controller.Response.Headers.RetryAfter.ToString(), Is.EqualTo("10"));
        Assert.That(orderBookService.LastOrder, Is.Null);
        Assert.That(await controller.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = DateTime.UtcNow }), Is.False);
    }

    [Test]
    public async Task ItemAndUserLookupsTrackReplacementCompletionAndRemoval()
    {
        var time = DateTime.UtcNow.AddSeconds(-8);
        var saved = new OrderEntry { UserId = "indexed-user", ItemId = "WHEAT", IsSell = true,
            Amount = 64, Filled = 64, IsEstimate = true, PricePerUnit = 10, Timestamp = time };
        await orderBookService.AddLoadedOrder(saved); // Estimated completion is no longer on the active queue.
        await orderBookService.AddLoadedOrder(new() { UserId = "other-user", ItemId = "WHEAT", IsSell = true,
            Amount = 32, PricePerUnit = 20, Timestamp = time });
        await orderBookService.AddLoadedOrder(new() { UserId = "indexed-user", ItemId = "DIAMOND", IsSell = true,
            Amount = 16, PricePerUnit = 10, Timestamp = time });
        await orderBookService.AddOrder(new() { UserId = saved.UserId, ItemId = saved.ItemId, IsSell = true,
            Amount = 64, Filled = 16, PricePerUnit = 10, Timestamp = time }, observed: true);
        var snapshot = orderBookService.GetUserOrders(saved.UserId);
        Assert.That(snapshot.Single(o => o.ItemId == "WHEAT").Filled, Is.EqualTo(16));
        snapshot.Single(o => o.ItemId == "WHEAT").Filled = 63;
        Assert.That(orderBookService.GetUserOrders(saved.UserId).Single(o => o.ItemId == "WHEAT").Filled, Is.EqualTo(16));
        await orderBookService.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = time.AddSeconds(1),
            SellOrders = new() { new() { PricePerUnit = 11, Amount = 10 } } });
        var filled = orderBookService.GetUserOrders(saved.UserId).Single(o => o.ItemId == "WHEAT");
        Assert.That(filled.Filled, Is.EqualTo(64));
        Assert.That(filled.IsEstimate, Is.False);
        Assert.That(orderBookService.GetUserOrders("other-user").Single().Filled, Is.Zero);
        Assert.That(orderBookService.GetUserOrders(saved.UserId).Single(o => o.ItemId == "DIAMOND").Filled, Is.Zero);
        await orderBookService.RemoveOrder(saved.ItemId, saved.UserId, saved.Timestamp);
        var publications = orderBookService.Published.Count;
        await orderBookService.MarkOrderFilled(saved.ItemId, saved.UserId, 10, 64);
        Assert.That(orderBookService.Published, Has.Count.EqualTo(publications), "Removed orders cannot be found by the item index");
        Assert.That(orderBookService.GetUserOrders(saved.UserId).Single().ItemId, Is.EqualTo("DIAMOND"));
    }

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task MarketChangePersistsFinalStateAndPublishesEveryAffectedUserOnce(bool kafka, bool sell)
    {
        var time = DateTime.UtcNow.AddSeconds(-5);
        for (var user = 0; user < 50; user++)
            for (var order = 0; order < 2; order++)
                await orderBookService.AddLoadedOrder(new() { UserId = "user-" + user, ItemId = "WHEAT",
                    Amount = 10, PricePerUnit = 10, IsSell = sell, Timestamp = time.AddMilliseconds(order) });
        await orderBookService.AddOrder(new() { UserId = "unaffected", ItemId = "OTHER",
            Amount = 10, PricePerUnit = 10, Timestamp = time });
        orderBookService.Published.Clear();
        var writes = new ConcurrentBag<OrderEntry>();
        orderBookService.OnWrite = async order => { await Task.Delay(30); writes.Add(order.Copy()); };
        var clock = System.Diagnostics.Stopwatch.StartNew();
        var price = sell ? 11 : 9; // Price has passed every tracked order.
        if (kafka)
            await orderBookService.BazaarPull(new() { Timestamp = DateTime.UtcNow, Products = new() {
                new() { ProductId = "WHEAT",
                    BuySummery = sell ? new() { new() { PricePerUnit = price, Amount = 100 } } : new(),
                    SellSummary = sell ? new() : new() { new() { PricePerUnit = price, Amount = 100 } } },
                new() { ProductId = "OTHER", BuySummery = new(), SellSummary = new() { new() { PricePerUnit = 10, Amount = 10 } } }
            } });
        else
            await orderBookService.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = DateTime.UtcNow,
                BuyOrders = sell ? null : new() { new() { PricePerUnit = price, Amount = 100 } },
                SellOrders = sell ? new() { new() { PricePerUnit = price, Amount = 100 } } : null });
        TestContext.Progress.WriteLine($"100 fills / 50 users with 30ms ledger I/O: {clock.Elapsed.TotalMilliseconds:F0}ms (kafka={kafka}, sell={sell})");
        Assert.That(writes, Has.Count.EqualTo(100), "Each final state is persisted once, including estimate-to-confirmed changes");
        Assert.That(writes.All(o => o.Filled == 10 && o.IsEstimate == false), Is.True);
        Assert.That(orderBookService.Published, Has.Count.EqualTo(50));
        for (var user = 0; user < 50; user++)
        {
            Assert.That(orderBookService.Published.Count(p => p.UserId == "user-" + user), Is.EqualTo(1));
            Assert.That(orderBookService.GetUserOrders("user-" + user).All(o => o.Filled == 10 && o.IsEstimate == false), Is.True);
        }
        Assert.That(orderBookService.GetUserOrders("unaffected").Single().Filled, Is.Zero);
    }

    [Test]
    public async Task SlowLedgerWriteDoesNotBlockOtherItemsDuringPersonalRefresh()
    {
        var blocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var progressed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        orderBookService.OnWrite = order => order.ItemId == "SLOW" ? blocked.Task : CompleteOtherItem();
        Task CompleteOtherItem() { progressed.TrySetResult(); return Task.CompletedTask; }
        var refresh = orderBookService.ObservePlayerOrders(new() { UserId = "1", PlayerName = "Ekwav", Timestamp = DateTime.UtcNow,
            Orders = new() { new() { ItemId = "SLOW", Amount = 10, PricePerUnit = 10, Timestamp = DateTime.UtcNow.AddSeconds(-5) },
                new() { ItemId = "FAST", Amount = 10, PricePerUnit = 10, Timestamp = DateTime.UtcNow.AddSeconds(-5) } } });
        try
        {
            await progressed.Task.WaitAsync(TimeSpan.FromSeconds(2));
            Assert.That(orderBookService.Published, Is.Empty, "Publish the complete view after all writes finish");
        }
        finally { blocked.TrySetResult(); await refresh; }
        Assert.That(orderBookService.Published, Has.Count.EqualTo(1));
        Assert.That(orderBookService.GetUserOrders("1"), Has.Count.EqualTo(2));
    }

    [Test, Explicit("Local CPU/allocation comparison for market matching and user snapshots")]
    public async Task MatchingAndSnapshotBenchmark()
    {
        var time = DateTime.UtcNow.AddMinutes(-1);
        var pull = new dev.BazaarPull { Timestamp = time.AddSeconds(1), Products = new() };
        for (var product = 0; product < 500; product++)
        {
            var tag = "BENCH_" + product;
            for (var order = 0; order < 20; order++)
                await orderBookService.AddLoadedOrder(new() { UserId = "bench-" + order, ItemId = tag,
                    IsSell = true, Amount = 64, PricePerUnit = 10, Timestamp = time.AddMilliseconds(order) });
            pull.Products.Add(new() { ProductId = tag,
                BuySummery = new() { new() { PricePerUnit = 10, Amount = 1280 } }, SellSummary = new() });
        }
        await orderBookService.BazaarPull(pull);
        var process = System.Diagnostics.Process.GetCurrentProcess();
        var cpu = process.TotalProcessorTime;
        var allocated = GC.GetTotalAllocatedBytes(true);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        for (var repeat = 0; repeat < 5; repeat++)
        {
            pull.Timestamp = pull.Timestamp.AddSeconds(1);
            await orderBookService.BazaarPull(pull);
            for (var user = 0; user < 20; user++)
                Assert.That(orderBookService.GetUserOrders("bench-" + user), Has.Count.EqualTo(500));
        }
        TestContext.Progress.WriteLine($"Bazaar benchmark: wall={clock.Elapsed.TotalMilliseconds:F1}ms cpu={(process.TotalProcessorTime - cpu).TotalMilliseconds:F1}ms allocated={GC.GetTotalAllocatedBytes(true) - allocated:N0} bytes");
    }

    private class CapturedLogs : ILogger<OrderBookService>
    {
        public readonly List<Dictionary<string, object>> Entries = new();
        public bool IsEnabled(LogLevel level) => true;
        public IDisposable BeginScope<TState>(TState state) where TState : notnull => null;
        public void Log<TState>(LogLevel level, EventId id, TState state, Exception exception, Func<TState, Exception, string> formatter) =>
            Entries.Add(((IEnumerable<KeyValuePair<string, object>>)state).ToDictionary(p => p.Key, p => p.Value));
    }

    [Test]
    public async Task DiagnosticsExplainEstimatedCompletionAndItsConfirmationWithoutChangingMatching()
    {
        var logs = new CapturedLogs();
        var service = new NoDbOrderBookService(null, messageApiMock.Object, itemsApiMock.Object, logs);
        var time = DateTime.UtcNow.AddSeconds(-8);
        var order = new OrderEntry { UserId = "diagnostic-user", ItemId = "WHEAT", Amount = 64, PricePerUnit = 10, Timestamp = time };
        await service.AddOrder(order);
        await service.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = time.AddSeconds(1), BuyOrders = new() { new() { PricePerUnit = 10, Amount = 32 } } });
        await service.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = time.AddSeconds(2), BuyOrders = new() { new() { PricePerUnit = 9, Amount = 64 } } });
        var confirmation = logs.Entries.Single(e => e.GetValueOrDefault("Reason") as string == "price_passed");
        Assert.That(confirmation["OrderId"], Is.EqualTo(OrderBookService.OrderId(order)));
        Assert.That(confirmation["PreviousEstimate"], Is.True);
        Assert.That(confirmation["IsEstimate"], Is.False);
        Assert.That(confirmation["Filled"], Is.EqualTo(64));
        Assert.That(confirmation["ObservedAt"], Is.EqualTo(time.AddSeconds(2)));
        var dropped = BazaarTelemetry.Observations.WithLabels("direct", "expired").Value;
        Assert.That(await service.UpdateOrderBook(new() { ItemTag = "WHEAT", Timestamp = DateTime.UtcNow.AddSeconds(-11) }), Is.False);
        Assert.That(BazaarTelemetry.Observations.WithLabels("direct", "expired").Value, Is.EqualTo(dropped + 1));
        Assert.That(logs.Entries.Any(e => e.GetValueOrDefault("Result") as string == "expired"), Is.True);
        Assert.That(service.GetUserOrders("diagnostic-user").Single().Filled, Is.EqualTo(64));
    }

}

