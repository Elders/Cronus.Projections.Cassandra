using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Serialization;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using DataStax = Cassandra;

namespace Elders.Cronus.Projections.Cassandra;

public class CassandraProvider : ICassandraProvider
{
    private CassandraProviderOptions _options;
    private readonly IKeyspaceNamingStrategy _keyspaceNamingStrategy;
    private readonly IInitializer _initializer;
    private readonly ILogger<CassandraProvider> _logger;

    private ICluster _cluster;
    private ISession _session;
    private string baseConfigurationKeyspace;

    private static readonly SemaphoreSlim ClusterThreadGate = new SemaphoreSlim(1, 1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time
    private static readonly SemaphoreSlim ThreadGate = new SemaphoreSlim(1, 1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time

    public CassandraProvider(IOptionsMonitor<CassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ILogger<CassandraProvider> logger, IInitializer initializer = null)
    {
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(keyspaceNamingStrategy);

        _options = optionsMonitor.CurrentValue;
        _keyspaceNamingStrategy = keyspaceNamingStrategy;
        _initializer = initializer;
        _logger = logger;
    }


    public async Task<ICluster> GetClusterAsync()
    {
        if (_cluster is not null )
            return _cluster;

        bool lockAcquired = false;

        try
        {
            lockAcquired = await ClusterThreadGate.WaitAsync(30000).ConfigureAwait(false);
            if (lockAcquired is false)
                throw new TimeoutException("Unable to accquire lock for casandra cluster.");

            if (_cluster is not null )
                return _cluster;

            Builder builder = _initializer as Builder;
            if (builder is null)
            {
                builder = DataStax.Cluster.Builder();
                //  TODO: check inside the `cfg` (var cfg = builder.GetConfiguration();) if we already have connectionString specified

                string connectionString = _options.ConnectionString;

                var hackyBuilder = new CassandraConnectionStringBuilder(connectionString);
                if (string.IsNullOrEmpty(hackyBuilder.DefaultKeyspace) == false)
                {
                    connectionString = connectionString.Replace(hackyBuilder.DefaultKeyspace, string.Empty);
                    baseConfigurationKeyspace = hackyBuilder.DefaultKeyspace;
                }
                else
                {
                    baseConfigurationKeyspace = _options.DefaultKeyspace;
                }

                var connStrBuilder = new CassandraConnectionStringBuilder(connectionString);

                const int thirtySeconds = 1000 * 30;
                SocketOptions so = new SocketOptions();
                so.SetReadTimeoutMillis(thirtySeconds);
                so.SetStreamMode(true);
                so.SetTcpNoDelay(true);

                _cluster = connStrBuilder
                    .ApplyToBuilder(builder)
                    .WithSocketOptions(so)
                    .WithTypeSerializers(new TypeSerializerDefinitions().Define(new ReadOnlyMemoryTypeSerializer()))
                    .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                    .WithCompression(CompressionType.LZ4)
                    .WithPoolingOptions(new PoolingOptions()
                            .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                            .SetMaxConnectionsPerHost(HostDistance.Local, 8)
                            .SetMaxRequestsPerConnection(_options.MaxRequestsPerConnection))
                    .Build();

                await _cluster.RefreshSchemaAsync().ConfigureAwait(false);
            }
            else
            {
                _cluster = DataStax.Cluster.BuildFrom(_initializer);
            }

            return _cluster;
        }
        finally
        {
            if (lockAcquired)
                ClusterThreadGate?.Release();
        }
    }

    public virtual string GetKeyspace()
    {
        return _keyspaceNamingStrategy.GetName(baseConfigurationKeyspace).ToLower();
    }

    public async Task<ISession> GetSessionAsync()
    {
        if (_session is null || _session.IsDisposed)
        {
            bool lockAcquired = false;

            try
            {
                lockAcquired = await ThreadGate.WaitAsync(30000).ConfigureAwait(false);
                if (lockAcquired == false)
                    throw new TimeoutException("Unable to acquire lock for getting cassandra session.");

                if (_session is null || _session.IsDisposed)
                {
                    if (_logger.IsEnabled(LogLevel.Information))
                        _logger.LogInformation("Refreshing cassandra session...");

                    ICluster cassandraCluster = await GetClusterAsync().ConfigureAwait(false);
                    _session = await cassandraCluster.ConnectAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                if (lockAcquired)
                    ThreadGate?.Release();
            }
        }

        return _session;
    }
}

class ReadOnlyMemoryTypeSerializer : CustomTypeSerializer<ReadOnlyMemory<byte>>
{
    public ReadOnlyMemoryTypeSerializer() : base("it doesn't matter") { }

    public override ReadOnlyMemory<byte> Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        => buffer.AsMemory(offset, length); // we will never get here because the byte[] serializer kicks in

    public override byte[] Serialize(ushort protocolVersion, ReadOnlyMemory<byte> value) => value.ToArray();
}
