using System.Collections.Concurrent;
using Cassandra;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Elders.Cronus.Projections.Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;

[SetUpFixture]
public class CassandraFixture : ICassandraProvider, IAsyncDisposable
{
    private static readonly ConcurrentDictionary<string, ISession> sessionPerKeyspace = [];
    private static ICluster cluster;
    private static readonly object mutex = new();

    public static IContainer Container { get; set; }

    [OneTimeSetUp]
    public async Task InitializeContainer()
    {
        Container = new ContainerBuilder()
            .WithImage("cassandra:4.1")
            .WithPortBinding(7000, true)
            .WithPortBinding(7001, true)
            .WithPortBinding(7199, true)
            .WithPortBinding(9042, true)
            .WithPortBinding(9160, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9042))
            .Build();

        await Container.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (Container is not null)
            await Container.DisposeAsync();
    }

    public Task<ICluster> GetClusterAsync()
    {
        if (cluster == null)
        {
            lock (mutex)
            {
                cluster ??= Cluster.Builder()
                        .AddContactPoint(Container.Hostname)
                        .WithTypeSerializers(new Cassandra.Serialization.TypeSerializerDefinitions().Define(new ReadOnlyMemoryTypeSerializer()))
                        .WithPort(Container.GetMappedPublicPort(9042))
                        .Build();
            }
        }

        return Task.FromResult(cluster);
    }

    public Task<ISession> GetSessionAsync()
    {
        string keyspace;
        if (TestContext.CurrentContext.Test.Type.Name.Length > 48)
            keyspace = TestContext.CurrentContext.Test.Type.Name[..48].ToLower();
        else
            keyspace = TestContext.CurrentContext.Test.Type.Name.ToLower();

        return GetSessionAsync(keyspace);
    }

    private async Task<ISession> GetSessionAsync(string keyspace)
    {
        if (sessionPerKeyspace.TryGetValue(keyspace, out ISession session) == false)
        {
            var cluster = await GetClusterAsync();
            session = await cluster.ConnectAsync();
            session.CreateKeyspaceIfNotExists(keyspace, new Dictionary<string, string>
                {
                    { "class", "SimpleStrategy" },
                    { "replication_factor", "1" }
                });
            session.ChangeKeyspace(keyspace);

            sessionPerKeyspace.TryAdd(keyspace, session);
        }

        return session;
    }

    public string GetKeyspace() => TestContext.CurrentContext.Test.Type.Name.ToLower();
}
