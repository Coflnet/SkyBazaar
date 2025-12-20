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
        public OrderEntry LastOrder { get; private set; }
        public OrderEntry RemovedOrder { get; private set; }
        public OrderEntry UpdatedOrder { get; private set; }
        public NoDbOrderBookService(ISessionContainer service, IMessageApi messageApi, IItemsApi itemsApi, ILogger<OrderBookService> logger) 
            : base(service, messageApi, itemsApi, logger)
        {
        }
        protected override Task InsertToDb(OrderEntry order)
        {
            LastOrder = order;
            return Task.CompletedTask;
        }
        protected override Task RemoveFromDb(OrderEntry order)
        {
            RemovedOrder = order;
            return Task.CompletedTask;
        }
        protected override Task UpdateInDb(OrderEntry order)
        {
            UpdatedOrder = order;
            return Task.CompletedTask;
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
            Timestamp = DateTime.UtcNow,
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
        Assert.That(orderbook.Sell.Where(o => o.UserId != null && o != newOrder).All(o => o.HasBeenNotified), Is.True);
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
    public async Task UpdateOrderBook_BuyOrderUndercut_ShouldNotifyAndRemove()
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

        // Update with lower price buy orders (undercut scenario)
        var update = new OrderBookUpdate()
        {
            ItemTag = "DIAMOND",
            Timestamp = DateTime.UtcNow,
            BuyOrders = new List<OrderEntry>
            {
                new() { Amount = 5, PricePerUnit = 99, IsSell = false }, // Undercutting order
                new() { Amount = 3, PricePerUnit = 98, IsSell = false }
            }
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.True, "Update should be accepted");
        
        // Verify notification was sent
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("user1", It.Is<MessageContainer>(m => 
            m.Message != null && System.Text.RegularExpressions.Regex.Replace(m.Message, "§.", "").Contains("undercut")), 0, default), Times.Once);
    }

    [Test]
    public async Task UpdateOrderBook_SellOrderOutbid_ShouldNotifyAndRemove()
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

        // Update with higher price sell orders (outbid scenario)
        var update = new OrderBookUpdate()
        {
            ItemTag = "EMERALD",
            Timestamp = DateTime.UtcNow,
            SellOrders = new List<OrderEntry>
            {
                new() { Amount = 5, PricePerUnit = 51, IsSell = true }, // Outbidding order
                new() { Amount = 3, PricePerUnit = 52, IsSell = true }
            }
        };

        var result = await orderBookService.UpdateOrderBook(update);

        Assert.That(result, Is.True, "Update should be accepted");
        
        // Verify notification was sent (looking for the generic outbid notification message)
        messageApiMock.Verify(m => m.MessageSendUserIdPostAsync("user2", It.IsAny<MessageContainer>(), 0, default), Times.Once);
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
    public async Task UpdateOrderBook_NoKafkaTimeYet_AcceptsAnyValidTimestamp()
    {
        // When no Kafka update time has been established yet, any valid timestamp should be accepted
        var update = new OrderBookUpdate()
        {
            ItemTag = "FRESH",
            Timestamp = DateTime.UtcNow.AddSeconds(-10), // Even older timestamp should work
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
        Assert.That(orderBook.Buy.Count, Is.EqualTo(1), "No new order should be added");
        Assert.That(orderBook.Buy[0].Amount, Is.EqualTo(15), "Amount should be updated to 15");
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
        
        var topOrder = orderBook.Buy.OrderByDescending(o => o.PricePerUnit).First();
        Assert.That(topOrder.Amount, Is.EqualTo(10), "Top order amount should be updated");
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
        
        // The update should remove only orders that:
        // 1. Fall within the min/max price range of the incoming update [134281.8, 134282.9]
        // 2. Are NOT in the incoming update
        // This handles filled orders while preserving orders outside the range
        //
        // Initial 11 orders:
        // - 134199.9 (below range) → kept
        // - 134200.0 (below range) → kept  
        // - 134281.8, 134281.9, 134282.0*, 134282.1, 134282.2, 134282.3, 134282.8, 134282.9 (8 in range, 7 in update)
        // - 134283.0 (above range) → kept
        // Result: 11 - 1 (134282.0) = 10 orders
        Assert.That(orderBook.Sell.Count, Is.EqualTo(10), "Should have 10 sell orders (1 removed within range, others preserved)");
        
        // Verify the correct order was removed
        var pricesAfterUpdate = orderBook.Sell.OrderBy(o => o.PricePerUnit).Select(o => o.PricePerUnit).ToList();
        Assert.That(pricesAfterUpdate, Contains.Item(134199.9), "Orders below update range should be preserved");
        Assert.That(pricesAfterUpdate, Contains.Item(134200.0), "Orders below update range should be preserved");
        Assert.That(pricesAfterUpdate, Does.Not.Contain(134282.0), "134282.0 should be removed (not in update, within range)");
        Assert.That(pricesAfterUpdate, Contains.Item(134283.0), "Orders above update range should be preserved");
    }
}

