using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Coflnet.Sky.SkyAuctionTracker.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using NUnit.Framework;

namespace Coflnet.Sky.SkyAuctionTracker.Controllers;

public class ConnectionReadyTests
{
    [Test]
    public async Task ConnectionDiagnosticReadsOnlySystemLocalAndDoesNotRelaxExistingReadinessGate()
    {
        var session = new Mock<ISession>();
        session.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>())).ReturnsAsync(new RowSet());
        var configuration = new ConfigurationBuilder().Build();
        using var services = new ServiceCollection().AddSingleton(session.Object)
            .AddSingleton(new BazaarService(configuration, NullLogger<BazaarService>.Instance, session.Object))
            .BuildServiceProvider();
        var controller = new MigrationController(services);
        Assert.That(await controller.ConnectionReady(CancellationToken.None), Is.TypeOf<OkObjectResult>());
        session.Verify(s => s.ExecuteAsync(It.Is<SimpleStatement>(statement =>
            statement.QueryString == "SELECT key FROM system.local" && statement.ReadTimeoutMillis == 5000)), Times.Once);
        Assert.That(((StatusCodeResult)controller.Ready()).StatusCode, Is.EqualTo(503));
    }

    [Test]
    public async Task BrokenConnectionFailsClosedWithoutExposingDriverDetails()
    {
        var session = new Mock<ISession>();
        session.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>()))
            .ThrowsAsync(new InvalidOperationException("synthetic-private-connection-detail"));
        using var services = new ServiceCollection().AddSingleton(session.Object).BuildServiceProvider();
        var response = (ObjectResult)await new MigrationController(services).ConnectionReady(CancellationToken.None);
        Assert.That(response.StatusCode, Is.EqualTo(503));
        Assert.That(response.Value, Is.EqualTo("not ready"));
    }

    [Test]
    public async Task HungDriverIsBoundedByFiveSecondHttpBudget()
    {
        var session = new Mock<ISession>();
        session.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>())).Returns(new TaskCompletionSource<RowSet>().Task);
        using var services = new ServiceCollection().AddSingleton(session.Object).BuildServiceProvider();
        var response = (ObjectResult)await new MigrationController(services).ConnectionReady(CancellationToken.None)
            .WaitAsync(TimeSpan.FromSeconds(7));
        Assert.That(response.StatusCode, Is.EqualTo(503));
    }
}
