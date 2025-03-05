using Cassandra;
using Elders.Cronus.MessageProcessing;
using Microsoft.Extensions.Logging.Abstractions;

namespace Elders.Cronus.Projections.Cassandra.Integration.Tests;

[TestFixture]
public class CassandraProjectionStoreSchemaNewTests
{
    ISession session;
    ICluster cluster;
    CassandraProjectionStoreSchemaNew projectionStore;
    ICronusContextAccessor contextAccessor;

    [SetUp]
    public async Task SetUp()
    {
        var cassandra = new CassandraFixture();
        session = await cassandra.GetSessionAsync();
        cluster = await cassandra.GetClusterAsync();
        projectionStore = new CassandraProjectionStoreSchemaNew(contextAccessor, cassandra, NullLogger<CassandraProjectionStoreSchemaNew>.Instance);
    }

    [Test]
    public void CreateProjectionStorageNewAsync()
    {
        Assert.DoesNotThrowAsync(async () => await projectionStore.CreateProjectionStorageNewAsync("tests"));

        var tables = cluster.Metadata.GetTables(session.Keyspace);
        Assert.That(tables, Contains.Item("tests"));
    }
}
