using Cassandra;
using Elders.Cronus.MessageProcessing;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;

namespace Elders.Cronus.Projections.Cassandra.Integration.Tests;

[TestFixture]
public class CassandraProjectionPartitionsStoreTests
{
    ISession session;
    CassandraProjectionPartitionsStore partitionsStore;
    private Mock<ICronusContextAccessor> contextAccessor;

    [SetUp]
    public async Task SetUp()
    {
        var bc = new BoundedContext { Name = "test" };
        var cassandra = new CassandraFixture();
        session = await cassandra.GetSessionAsync();

        contextAccessor = new Mock<ICronusContextAccessor>();
        var serviceProviderMock = new Mock<IServiceProvider>();
        var cronusContext = new CronusContext("test", serviceProviderMock.Object);
        contextAccessor.SetupProperty(x => x.CronusContext, cronusContext);

        partitionsStore = new CassandraProjectionPartitionsStore(contextAccessor.Object, cassandra, NullLogger<CassandraProjectionPartitionsStore>.Instance);

        var partitionStoreSchema = new CassandraProjectionPartitionStoreSchema(contextAccessor.Object, cassandra, NullLogger<CassandraProjectionPartitionStoreSchema>.Instance);
        await partitionStoreSchema.CreateProjectionPartitionsStorage();
    }

    [Test]
    public async Task AppendAsync()
    {
        var id = TestId.New().RawId.ToArray();
        await partitionsStore.AppendAsync(new ProjectionPartition("proj", id, 1));

        var rows = await session.ExecuteAsync(new SimpleStatement("SELECT pid FROM projection_partitions WHERE pt=? AND id=?;", "proj", id));
        var row = rows.SingleOrDefault();

        Assert.Multiple(() =>
        {
            Assert.That(row, Is.Not.Null);
            Assert.That(row.GetValue<long>("pid"), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task GetPartitionsAsync()
    {
        var id = TestId.New();
        var idBytes = id.RawId.ToArray();
        await partitionsStore.AppendAsync(new ProjectionPartition("proj", idBytes, 2));
        await partitionsStore.AppendAsync(new ProjectionPartition("proj", idBytes, 1));
        await partitionsStore.AppendAsync(new ProjectionPartition("proj", idBytes, 3));

        var partitions = await partitionsStore.GetPartitionsAsync("proj", id);

        Assert.Multiple(() =>
        {
            Assert.That(partitions, Has.Count.EqualTo(3));
            Assert.That(partitions, Has.ItemAt(0).EqualTo(1));
            Assert.That(partitions, Has.ItemAt(1).EqualTo(2));
            Assert.That(partitions, Has.ItemAt(2).EqualTo(3));
        });
    }
}
