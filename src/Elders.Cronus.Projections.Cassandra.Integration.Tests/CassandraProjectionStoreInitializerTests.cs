using System.Security.Cryptography;
using System.Text;
using Cassandra;
using Elders.Cronus.EventStore;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;

namespace Elders.Cronus.Projections.Cassandra.Integration.Tests;

[TestFixture]
public class CassandraProjectionStoreInitializerTests
{
    ISession session;
    ICluster cluster;
    VersionedProjectionsNaming naming;
    CassandraProjectionStoreInitializer initializer;

    [SetUp]
    public async Task SetUp()
    {
        var cassandra = new CassandraFixture();
        session = await cassandra.GetSessionAsync();
        cluster = await cassandra.GetClusterAsync();
        naming = new VersionedProjectionsNaming();
        var projectionStore = new CassandraProjectionStoreSchema(cassandra, NullLogger<CassandraProjectionStoreSchema>.Instance);
        var partitionSchema = new CassandraProjectionPartitionStoreSchema(cassandra, NullLogger<CassandraProjectionPartitionStoreSchema>.Instance);
        var projectionStoreNew = new CassandraProjectionStoreSchemaNew(cassandra, NullLogger<CassandraProjectionStoreSchemaNew>.Instance);
        initializer = new CassandraProjectionStoreInitializer(projectionStore, partitionSchema, naming, projectionStoreNew);
    }

    [Test]
    public async Task InitializeAsync()
    {
        var version = new ProjectionVersion("proj", ProjectionStatus.Live, 1, "hash");
        var result = await initializer.InitializeAsync(version);

        var tables = cluster.Metadata.GetTables(session.Keyspace);
        var cf = naming.GetColumnFamily(version);
        var cfNew = naming.GetColumnFamilyNew(version);

        Assert.That(result, Is.True);
        Assert.That(tables, Contains.Item("projection_partitions"));
        Assert.That(tables, Contains.Item(cf));
        Assert.That(tables, Contains.Item(cfNew));
    }
}

[TestFixture]
public class CassandraProjectionStoreNewTests
{
    ISession session;
    CassandraProjectionStoreNew projectionStore;
    VersionedProjectionsNaming naming;
    ISerializer serializer;

    ProjectionVersion version;

    [SetUp]
    public async Task SetUp()
    {
        var cassandra = new CassandraFixture();
        session = await cassandra.GetSessionAsync();
        var partitionStore = new CassandraProjectionPartitionsStore(cassandra, NullLogger<CassandraProjectionPartitionsStore>.Instance);
        serializer = new SerializerMock();
        naming = new VersionedProjectionsNaming();
        projectionStore = new CassandraProjectionStoreNew(cassandra, partitionStore, serializer, naming, NullLogger<CassandraProjectionStoreNew>.Instance);

        var projectionStoreSchema = new CassandraProjectionStoreSchema(cassandra, NullLogger<CassandraProjectionStoreSchema>.Instance);
        var partitionSchema = new CassandraProjectionPartitionStoreSchema(cassandra, NullLogger<CassandraProjectionPartitionStoreSchema>.Instance);
        var projectionStoreNew = new CassandraProjectionStoreSchemaNew(cassandra, NullLogger<CassandraProjectionStoreSchemaNew>.Instance);
        var initializer = new CassandraProjectionStoreInitializer(projectionStoreSchema, partitionSchema, naming, projectionStoreNew);
        version = new ProjectionVersion("proj", ProjectionStatus.Live, 1, "hash");

        await initializer.InitializeAsync(version);
    }

    [Test]
    public async Task SaveAsync()
    {
        var projectionId = TestId.New();
        var @event = new TestEvent(projectionId, DateTimeOffset.UtcNow);
        var commit = new ProjectionCommit(projectionId, version, @event);

        Assert.DoesNotThrowAsync(async () => await projectionStore.SaveAsync(commit));

        var cfNew = naming.GetColumnFamilyNew(version);
        long partitionId = @event.Timestamp.Year * 100 + @event.Timestamp.Month;
        var rows = await session.ExecuteAsync(new SimpleStatement($"SELECT pid,data,ts FROM {cfNew} WHERE id=? AND pid=?;", projectionId.RawId, partitionId));
        var count = 0;

        using var scope = Assert.EnterMultipleScope();
        foreach (var row in rows)
        {
            count++;
            var data = row.GetValue<byte[]>("data");
            var ts = row.GetValue<long>("ts");

            Assert.That(data, Is.EquivalentTo(serializer.SerializeToBytes(@event)));
            Assert.That(ts, Is.EqualTo(@event.Timestamp.ToFileTime()));
        }

        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public async Task EnumerateProjectionsAsOfDateAsync()
    {
        DateTimeOffset timestamp1 = new DateTimeOffset(2023, 10, 18, 0, 0, 0, TimeSpan.Zero);
        DateTimeOffset timestamp2 = new DateTimeOffset(2023, 11, 18, 0, 0, 0, TimeSpan.Zero);
        DateTimeOffset timestamp3 = new DateTimeOffset(2023, 11, 20, 0, 0, 0, TimeSpan.Zero);
        DateTimeOffset timestamp4 = new DateTimeOffset(2023, 12, 10, 0, 0, 0, TimeSpan.Zero);

        DateTimeOffset asOfTimestamp = new DateTimeOffset(2023, 11, 22, 0, 0, 0, TimeSpan.Zero);

        var projectionId = TestId.New();
        var @event1 = new TestEvent(projectionId, timestamp1);
        var commit1 = new ProjectionCommit(projectionId, version, @event1);
        await projectionStore.SaveAsync(commit1);

        var @event2 = new TestEvent(projectionId, timestamp2);
        var commit2 = new ProjectionCommit(projectionId, version, @event2);
        await projectionStore.SaveAsync(commit2);

        var @event3 = new TestEvent(projectionId, timestamp3);
        var commit3 = new ProjectionCommit(projectionId, version, @event3);
        await projectionStore.SaveAsync(commit3);

        var @event4 = new TestEvent(projectionId, timestamp4);
        var commit4 = new ProjectionCommit(projectionId, version, @event4);
        await projectionStore.SaveAsync(commit4);

        var eventsInStream = 0;
        DateTimeOffset timestampOfLastLoadedEvent = DateTimeOffset.MinValue;

        await projectionStore.EnumerateProjectionsAsync(new ProjectionsOperator
        {
            OnProjectionStreamLoadedAsync = stream =>
            {
                eventsInStream = stream.Count();
                timestampOfLastLoadedEvent = stream.Last().Timestamp;

                return Task.CompletedTask;
            }
        }, new ProjectionQueryOptions(projectionId, version, asOfTimestamp));

        Assert.Multiple(() =>
        {
            Assert.That(eventsInStream, Is.EqualTo(3));
            Assert.That(timestampOfLastLoadedEvent, Is.EqualTo(timestamp3));
        });
    }

    [Test]
    public async Task EnumerateProjectionsWithPaging()
    {
        DateTimeOffset timestamp = DateTimeOffset.UtcNow.AddMonths(2);

        var projectionId = TestId.New();
        var @event1 = new TestEvent(projectionId, DateTimeOffset.UtcNow);
        var commit1 = new ProjectionCommit(projectionId, version, @event1);
        await projectionStore.SaveAsync(commit1);

        var @event2 = new TestEvent(projectionId, DateTimeOffset.UtcNow.AddDays(20));
        var commit2 = new ProjectionCommit(projectionId, version, @event2);
        await projectionStore.SaveAsync(commit2);

        var @event3 = new TestEvent(projectionId, DateTimeOffset.UtcNow.AddMonths(1).AddDays(2));
        var commit3 = new ProjectionCommit(projectionId, version, @event3);
        await projectionStore.SaveAsync(commit3);

        var @event4 = new TestEvent(projectionId, timestamp);
        var commit4 = new ProjectionCommit(projectionId, version, @event4);
        await projectionStore.SaveAsync(commit4);

        int numberOfEventsFirstLoad = 0;
        int numberOfEventsSecondLoad = 0;

        byte[] firstPagingToken = null;
        byte[] secondPagingToken = null;

        DateTimeOffset lastEventTimestamp = default;

        await projectionStore.EnumerateProjectionsAsync(new ProjectionsOperator
        {
            OnProjectionStreamLoadedWithPagingAsync = (stream, options) =>
            {
                numberOfEventsFirstLoad = stream.Count();
                firstPagingToken = options.PaginationToken;
                return Task.CompletedTask;
            }
        }, new ProjectionQueryOptions(projectionId, version, new PagingOptions(3, null, Order.Ascending))); // first load

        await projectionStore.EnumerateProjectionsAsync(new ProjectionsOperator
        {
            OnProjectionStreamLoadedWithPagingAsync = (stream, options) =>
            {
                numberOfEventsSecondLoad = stream.Count();
                secondPagingToken = options.PaginationToken;
                lastEventTimestamp = stream.Last().Timestamp;

                return Task.CompletedTask;
            }
        }, new ProjectionQueryOptions(projectionId, version, new PagingOptions(2, firstPagingToken, Order.Ascending))); // second load with token

        Assert.Multiple(() =>
        {
            Assert.That(numberOfEventsFirstLoad, Is.EqualTo(3));
            Assert.That(numberOfEventsSecondLoad, Is.EqualTo(1));
            Assert.That(lastEventTimestamp, Is.EqualTo(timestamp));

            Assert.That(firstPagingToken, Is.Not.Null);
            Assert.That(secondPagingToken, Is.Null);
        });
    }

    [Test]
    public async Task EnumerateProjectionsWithPagingDescending()
    {
        DateTimeOffset timestamp = DateTimeOffset.UtcNow;

        var projectionId = TestId.New();
        var @event1 = new TestEvent(projectionId, timestamp);
        var commit1 = new ProjectionCommit(projectionId, version, @event1);
        await projectionStore.SaveAsync(commit1);

        var @event2 = new TestEvent(projectionId, DateTimeOffset.UtcNow.AddDays(20));
        var commit2 = new ProjectionCommit(projectionId, version, @event2);
        await projectionStore.SaveAsync(commit2);

        var @event3 = new TestEvent(projectionId, DateTimeOffset.UtcNow.AddMonths(1).AddDays(2));
        var commit3 = new ProjectionCommit(projectionId, version, @event3);
        await projectionStore.SaveAsync(commit3);

        var @event4 = new TestEvent(projectionId, DateTimeOffset.UtcNow.AddMonths(2));
        var commit4 = new ProjectionCommit(projectionId, version, @event4);
        await projectionStore.SaveAsync(commit4);

        int numberOfEventsFirstLoad = 0;
        int numberOfEventsSecondLoad = 0;

        byte[] firstPagingToken = null;
        byte[] secondPagingToken = null;

        DateTimeOffset lastEventTimestamp = default;

        await projectionStore.EnumerateProjectionsAsync(new ProjectionsOperator
        {
            OnProjectionStreamLoadedWithPagingAsync = (stream, options) =>
            {
                numberOfEventsFirstLoad = stream.Count();
                firstPagingToken = options.PaginationToken;
                return Task.CompletedTask;
            }
        }, new ProjectionQueryOptions(projectionId, version, new PagingOptions(2, null, Order.Descending))); // first load

        await projectionStore.EnumerateProjectionsAsync(new ProjectionsOperator
        {
            OnProjectionStreamLoadedWithPagingAsync = (stream, options) =>
            {
                numberOfEventsSecondLoad = stream.Count();
                secondPagingToken = options.PaginationToken;
                lastEventTimestamp = stream.Last().Timestamp;

                return Task.CompletedTask;
            }
        }, new ProjectionQueryOptions(projectionId, version, new PagingOptions(3, firstPagingToken, Order.Descending))); // second load with token

        Assert.Multiple(() =>
        {
            Assert.That(numberOfEventsFirstLoad, Is.EqualTo(2));
            Assert.That(numberOfEventsSecondLoad, Is.EqualTo(2));
            Assert.That(lastEventTimestamp, Is.EqualTo(timestamp));

            Assert.That(firstPagingToken, Is.Not.Null);
            Assert.That(secondPagingToken, Is.Null);
        });
    }

    [Test]
    public async Task EnumerateProjectionsAsync()
    {
        var projectionId = TestId.New();
        var @event1 = new TestEvent(projectionId, DateTimeOffset.UtcNow);
        var commit1 = new ProjectionCommit(projectionId, version, @event1);
        await projectionStore.SaveAsync(commit1);

        var @event2 = new TestEvent(projectionId, DateTimeOffset.UtcNow.AddSeconds(1));
        var commit2 = new ProjectionCommit(projectionId, version, @event2);
        await projectionStore.SaveAsync(commit2);

        var eventsInStream = 0;
        var secondEnumerationPerformed = false;
        await projectionStore.EnumerateProjectionsAsync(new ProjectionsOperator
        {
            OnProjectionStreamLoadedAsync = stream =>
            {
                eventsInStream = stream.Count();
                return Task.CompletedTask;
            },
            OnProjectionStreamLoadedWithPagingAsync = (stream, ops) =>
            {
                secondEnumerationPerformed = true;
                return Task.CompletedTask;
            }
        }, new ProjectionQueryOptions(projectionId, version, new PagingOptions(100, null, Order.Ascending)));

        Assert.Multiple(() =>
        {
            Assert.That(eventsInStream, Is.EqualTo(2));
            Assert.That(secondEnumerationPerformed, Is.False);
        });
    }
}

sealed class TestId : IBlobId
{
    public TestId(ReadOnlyMemory<byte> rawId)
    {
        RawId = rawId;
    }

    public ReadOnlyMemory<byte> RawId { get; }

    public static TestId New()
    {
        Memory<byte> memory = new byte[64];
        RandomNumberGenerator.Fill(memory.Span);
        return new TestId(memory);
    }
}

sealed class TestEvent : IEvent
{
    public TestEvent(TestId id, DateTimeOffset timestamp)
    {
        Id = id;
        Timestamp = timestamp;
    }

    public TestId Id { get; }

    public DateTimeOffset Timestamp { get; }
}

class SerializerMock : ISerializer
{
    public SerializerMock()
    {
        settings = new JsonSerializerSettings
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
            DateFormatHandling = DateFormatHandling.IsoDateFormat,
            TypeNameHandling = TypeNameHandling.Objects,
            TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple,
            Formatting = Formatting.None,
            ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor
        };
    }

    private readonly JsonSerializerSettings settings;

    public T DeserializeFromBytes<T>(byte[] bytes)
    {
        var json = Encoding.UTF8.GetString(bytes);
        return JsonConvert.DeserializeObject<T>(json, settings);
    }

    public byte[] SerializeToBytes<T>(T message)
    {
        var json = JsonConvert.SerializeObject(message, settings);
        return Encoding.UTF8.GetBytes(json);
    }

    public string SerializeToString<T>(T message)
    {
        return JsonConvert.SerializeObject(message, settings);
    }
}
