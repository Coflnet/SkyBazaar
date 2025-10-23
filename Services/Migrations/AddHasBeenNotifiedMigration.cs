using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Coflnet.Sky.SkyAuctionTracker.Services.Migrations;

/// <summary>
/// Simple startup migration: ensure the order_book table contains the has_been_notified column.
/// Runs once at application start.
/// </summary>
public class AddHasBeenNotifiedMigration : IHostedService
{
    private readonly ISession session;
    private readonly ILogger<AddHasBeenNotifiedMigration> logger;

    public AddHasBeenNotifiedMigration(ISession session, ILogger<AddHasBeenNotifiedMigration> logger)
    {
        this.session = session;
        this.logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return Task.Run(async () =>
        {
            try
            {
                // Check whether the column already exists in the current keyspace's order_book table.
                var keyspace = session.Keyspace;
                if (string.IsNullOrWhiteSpace(keyspace))
                {
                    logger.LogWarning("No keyspace set on session; skipping has_been_notified migration.");
                    return;
                }

                var checkCql = $"SELECT column_name FROM system_schema.columns WHERE keyspace_name='{keyspace}' AND table_name='order_book' AND column_name='has_been_notified'";
                try
                {
                    var rs = await session.ExecuteAsync(new SimpleStatement(checkCql).SetConsistencyLevel(ConsistencyLevel.Quorum));
                    if (rs != null && rs.GetRows().GetEnumerator().MoveNext())
                    {
                        logger.LogInformation("Migration skipped: column 'has_been_notified' already exists in {keyspace}.order_book", keyspace);
                        return;
                    }
                }
                catch (Exception e)
                {
                    // If the metadata query fails (old Cassandra versions), fall back to attempting ALTER and catch errors.
                    logger.LogWarning(e, "Could not query system_schema.columns; will attempt ALTER TABLE as fallback.");
                }

                // Column wasn't found, attempt to add it.
                var cql = $"ALTER TABLE {keyspace}.order_book ADD has_been_notified boolean";
                logger.LogInformation("Running migration: {cql}", cql);
                try
                {
                    await session.ExecuteAsync(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.Quorum));
                    logger.LogInformation("Migration succeeded: added has_been_notified column.");
                }
                catch (InvalidQueryException iqe)
                {
                    // Column may already exist or ALTER not supported; log and continue
                    logger.LogWarning(iqe, "Migration ALTER TABLE failed - possibly column already exists or unsupported CQL version.");
                }
                catch (Exception e)
                {
                    logger.LogError(e, "Unexpected error while running migration to add has_been_notified");
                }
            }
            catch (Exception e)
            {
                logger.LogError(e, "Migration task failed");
            }
        }, cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
