using Cassandra;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Elders.Cronus.Projections.Cassandra.Integration.Tests;

[TestFixture]
public class CassandraProjectionStoreSchemaNewTests
{
    ISession session;
    ICluster cluster;
    CassandraProjectionStoreSchemaNew projectionStore;

    [SetUp]
    public async Task SetUp()
    {
        var cassandra = new CassandraFixture();
        session = await cassandra.GetSessionAsync();
        cluster = await cassandra.GetClusterAsync();
        projectionStore = new CassandraProjectionStoreSchemaNew(cassandra, new NullLoggerFactory().CreateLogger<CassandraProjectionStoreSchemaNew>());
    }

    [Test]
    public void CreateProjectionStorageNewAsync()
    {
        Assert.DoesNotThrowAsync(async () => await projectionStore.CreateProjectionStorageNewAsync("tests"));

        var tables = cluster.Metadata.GetTables(session.Keyspace);
        Assert.That(tables, Contains.Item("tests"));
    }
}
