using NUnit.Framework;
using Moq;
using Cassandra;
using Coflnet.Sky.EventBroker.Client.Api;
using Coflnet.Sky.Items.Client.Api;
using Coflnet.Sky.SkyBazaar.Models;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
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
}
