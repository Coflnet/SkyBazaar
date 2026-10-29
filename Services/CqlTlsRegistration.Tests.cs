extern alias CoflCore;

using System;
using System.Collections.Generic;
using CoflCore::Coflnet.Core.Security;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;

namespace Coflnet.Sky.SkyAuctionTracker.Services;

public class CqlTlsRegistrationTests
{
    [Test]
    public void PrimaryAndOptionalMigrationHaveIndependentProcessOwnedPemSources()
    {
        var values = new Dictionary<string, string>
        {
            ["MARIADB_VERSION"] = "10.11.6", ["DBCONNECTION"] = "Server=synthetic-only",
            ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = "http://127.0.0.1:4318"
        };
        foreach (var name in new[] { "CASSANDRA", "OLD_CASSANDRA" })
            foreach (var key in new[] { "CA_CERT_PATH", "CERT_PATH", "KEY_PATH" })
                values[name + ":TLS:" + key] = "/synthetic-missing/" + name + "/" + key;
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        var collection = new ServiceCollection().AddSingleton<IConfiguration>(configuration);
        new Startup(configuration).ConfigureServices(collection);
        var provider = collection.BuildServiceProvider();
        var primary = provider.GetRequiredKeyedService<PemTlsOptions>("CASSANDRA");
        var old = provider.GetRequiredKeyedService<PemTlsOptions>("OLD_CASSANDRA");
        try
        {
            Assert.That(old, Is.Not.SameAs(primary));
        }
        finally { provider.Dispose(); }
        Assert.Throws<ObjectDisposedException>(() => primary.GetCertificate());
        Assert.Throws<ObjectDisposedException>(() => old.GetCertificate());
    }
}
