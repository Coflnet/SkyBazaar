using System;
using System.Threading.Tasks;
using Cassandra;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.SkyAuctionTracker.Services.Migrations;

internal static class AddHasBeenNotifiedMigration
{
    internal static async Task EnsureColumns(ISession session, ILogger logger)
    {
        var keyspace = session.Keyspace;
        if (string.IsNullOrWhiteSpace(keyspace))
            throw new InvalidOperationException("Order-book session has no keyspace");
        foreach (var (column, type) in new[] { ("has_been_notified", "boolean"), ("is_estimate", "boolean"), ("is_expired", "boolean"), ("claimed", "int") })
        {
            var query = new SimpleStatement($"SELECT column_name FROM system_schema.columns WHERE keyspace_name='{keyspace}' AND table_name='order_book' AND column_name='{column}'");
            if ((await session.ExecuteAsync(query)).GetRows().GetEnumerator().MoveNext())
                continue;
            try
            {
                await session.ExecuteAsync(new SimpleStatement($"ALTER TABLE {keyspace}.order_book ADD {column} {type}"));
                logger.LogInformation("Added order_book column {Column}", column);
            }
            catch (InvalidQueryException)
            {
                // A concurrent startup may have completed the same additive migration.
                if (!(await session.ExecuteAsync(query)).GetRows().GetEnumerator().MoveNext())
                    throw;
            }
        }
    }
}
