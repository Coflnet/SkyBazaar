using System.Threading.Tasks;
using Cassandra;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using NUnit.Framework;

namespace Coflnet.Sky.SkyAuctionTracker.Services.Migrations;

public class AddHasBeenNotifiedMigrationTests
{
    [Test]
    public async Task MissingColumnsAreAddedUsingTheOrderBooksActualKeyspace()
    {
        var session = new Mock<ISession>();
        session.SetupGet(s => s.Keyspace).Returns("bazaar_test");
        session.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>())).ReturnsAsync(new RowSet());
        await AddHasBeenNotifiedMigration.EnsureColumns(session.Object, NullLogger.Instance);
        session.Verify(s => s.ExecuteAsync(It.Is<SimpleStatement>(q => q.QueryString ==
            "ALTER TABLE bazaar_test.order_book ADD is_estimate boolean")), Times.Once);
        session.Verify(s => s.ExecuteAsync(It.Is<SimpleStatement>(q => q.QueryString ==
            "ALTER TABLE bazaar_test.order_book ADD has_been_notified boolean")), Times.Once);
    }

    [Test]
    public void FailedSchemaChangeKeepsStartupUnready()
    {
        var session = new Mock<ISession>();
        session.SetupGet(s => s.Keyspace).Returns("bazaar_test");
        session.Setup(s => s.ExecuteAsync(It.IsAny<IStatement>())).ReturnsAsync(new RowSet());
        session.Setup(s => s.ExecuteAsync(It.Is<SimpleStatement>(q => q.QueryString.StartsWith("ALTER"))))
            .ThrowsAsync(new InvalidQueryException("permission denied"));
        Assert.ThrowsAsync<InvalidQueryException>(() => AddHasBeenNotifiedMigration.EnsureColumns(session.Object, NullLogger.Instance));
    }
}
