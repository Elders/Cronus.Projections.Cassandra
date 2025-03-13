using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection.PortableExecutable;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra;

public interface ICassandraProjectionStoreSchemaNew
{
    Task CreateProjectionStorageNewAsync(string location);
}

public class CassandraProjectionStoreSchemaNew : ICassandraProjectionStoreSchemaNew
{
    private readonly ConcurrentDictionary<string, bool> initializedLocations;

    private readonly ILogger<CassandraProjectionStoreSchemaNew> logger;
    private readonly ICassandraProvider cassandraProvider;
    private readonly ICassandraReplicationStrategy replicationStrategy;
    private readonly ICronusContextAccessor _cronusContextAccessor;
    //                                                                                       
    const string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS {0}.""{1}"" (id blob, pid bigint, data blob, ts bigint, PRIMARY KEY ((id, pid), ts)) WITH CLUSTERING ORDER BY (ts ASC);";
    const string DropQueryTemplate = @"DROP TABLE IF EXISTS {0};";

    private CreateTablePreparedStatementNew _createTablePreparedStatementNew;

    private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();
    public async Task<string> GetKeypaceAsync()
    {
        ISession session = await GetSessionAsync().ConfigureAwait(false);
        return session.Keyspace;
    }

    /// <summary>
    /// Used for cassandra schema changes exclusively
    /// https://issues.apache.org/jira/browse/CASSANDRA-10699
    /// https://issues.apache.org/jira/browse/CASSANDRA-11429
    /// </summary>
    /// <param name="sessionForSchemaChanges"></param>
    public CassandraProjectionStoreSchemaNew(ICronusContextAccessor cronusContextAccessor, ICassandraProvider cassandraProvider, ICassandraReplicationStrategy replicationStrategy, ILogger<CassandraProjectionStoreSchemaNew> logger)
    {
        if (ReferenceEquals(null, cassandraProvider)) throw new ArgumentNullException(nameof(cassandraProvider));
        this.cassandraProvider = cassandraProvider;
        this.replicationStrategy = replicationStrategy;
        this.logger = logger;

        _cronusContextAccessor = cronusContextAccessor;
        _createTablePreparedStatementNew = new CreateTablePreparedStatementNew(cronusContextAccessor, cassandraProvider);

        initializedLocations = new ConcurrentDictionary<string, bool>();
    }

    public async Task CreateTableAsync(ISession session, string location)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("[Projections] Creating table `{tableName}` with `{address}`...", location, session.Cluster.AllHosts().First().Address);

        PreparedStatement statement = await _createTablePreparedStatementNew.PrepareStatementAsync(session, location);
        await session.ExecuteAsync(statement.Bind()).ConfigureAwait(false);

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("[Projections] Created table `{tableName}`... Maybe?!", location);
    }

    public async Task CreateProjectionStorageNewAsync(string location)
    {
        ISession session = await GetSessionAsync().ConfigureAwait(false);

        await CreateKeyspace(session).ConfigureAwait(false);

        string key = $"{_cronusContextAccessor.CronusContext.Tenant}_{location}"; // because this class is singleton
        if (initializedLocations.TryGetValue(key, out bool isInitialized))
        {
            if (isInitialized == false)
            {
                await CreateTableAsync(session, location).ConfigureAwait(false);
                initializedLocations.TryUpdate(key, true, false);
            }
        }
        else
        {
            await CreateTableAsync(session, location).ConfigureAwait(false);
            initializedLocations.TryAdd(key, true);
        }
    }

    public async Task CreateKeyspace(ISession session)
    {
        IStatement createTableStatement = await GetCreateKeySpaceQuery(session).ConfigureAwait(false);
        await session.ExecuteAsync(createTableStatement).ConfigureAwait(false);
    }

    private async Task<IStatement> GetCreateKeySpaceQuery(ISession session)
    {
        string keyspace = cassandraProvider.GetKeyspace();
        string createKeySpaceQueryTemplate = replicationStrategy.CreateKeySpaceTemplate(keyspace);
        PreparedStatement createEventsTableStatement = await session.PrepareAsync(createKeySpaceQueryTemplate).ConfigureAwait(false);
        createEventsTableStatement.SetConsistencyLevel(ConsistencyLevel.All);

        return createEventsTableStatement.Bind();
    }

    class CreateTablePreparedStatementNew : PreparedStatementCache
    {
        public CreateTablePreparedStatementNew(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
        { }
        internal override string GetQueryTemplate() => CreateProjectionEventsTableTemplate;
    }
}
