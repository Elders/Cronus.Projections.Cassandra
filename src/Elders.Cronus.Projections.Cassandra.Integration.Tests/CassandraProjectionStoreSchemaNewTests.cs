using Cassandra;
using Elders.Cronus.MessageProcessing;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Elders.Cronus.Projections.Cassandra.Integration.Tests;

[TestFixture]
public class CassandraProjectionStoreSchemaNewTests
{
    ISession session;
    ICluster cluster;
    CassandraProjectionStoreSchemaNew projectionStore;
    private Mock<ICronusContextAccessor> contextAccessor;

    [SetUp]
    public async Task SetUp()
    {
        var cassandra = new CassandraFixture();
        session = await cassandra.GetSessionAsync();
        cluster = await cassandra.GetClusterAsync();

        contextAccessor = new Mock<ICronusContextAccessor>();
        var serviceProviderMock = new Mock<IServiceProvider>();
        var cronusContext = new CronusContext("test", serviceProviderMock.Object);
        contextAccessor.SetupProperty(x => x.CronusContext, cronusContext);

        projectionStore = new CassandraProjectionStoreSchemaNew(contextAccessor.Object, cassandra, NullLogger<CassandraProjectionStoreSchemaNew>.Instance);
    }

    [Test]
    public void CreateProjectionStorageNewAsync()
    {
        Assert.DoesNotThrowAsync(async () => await projectionStore.CreateProjectionStorageNewAsync("tests"));

        var tables = cluster.Metadata.GetTables(session.Keyspace);
        Assert.That(tables, Contains.Item("tests"));
    }
}
