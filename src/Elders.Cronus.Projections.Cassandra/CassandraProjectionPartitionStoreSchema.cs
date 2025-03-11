using Cassandra;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Elders.Cronus.Projections.Cassandra;

public interface ICassandraProjectionPartitionStoreSchema
{
    Task CreateProjectionPartitionsStorage();
}

public class CassandraProjectionPartitionStoreSchema : ICassandraProjectionPartitionStoreSchema
{
    const string CreateProjectionPartionsTableTemplate = @"CREATE TABLE IF NOT EXISTS {0}.""{1}"" (pt text, id blob, pid bigint, PRIMARY KEY ((pt,id), pid)) WITH CLUSTERING ORDER BY (pid ASC)";
    const string PartionsTableName = "projection_partitions";

    private readonly ILogger<CassandraProjectionPartitionStoreSchema> logger;
    private readonly ICassandraProvider cassandraProvider;
    private CreateTablePreparedStatement _createTablePreparedStatement;

    public CassandraProjectionPartitionStoreSchema(ICronusContextAccessor cronusContextAccessor, ICassandraProvider cassandraProvider, ILogger<CassandraProjectionPartitionStoreSchema> logger)
    {
        if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

        this.cassandraProvider = cassandraProvider;
        this.logger = logger;

        _createTablePreparedStatement = new CreateTablePreparedStatement(cronusContextAccessor, cassandraProvider);
    }

    private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();

    public async Task CreateProjectionPartitionsStorage()
    {
        ISession session = await GetSessionAsync().ConfigureAwait(false);
        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("[EventStore] Creating table `{tableName}` with `{address}` in keyspace `{keyspace}`...", PartionsTableName, session.Cluster.AllHosts().First().Address, session.Keyspace);

        PreparedStatement createEventsTableStatement = await _createTablePreparedStatement.PrepareStatementAsync(session, PartionsTableName);

        await session.ExecuteAsync(createEventsTableStatement.Bind()).ConfigureAwait(false);

        if (logger.IsEnabled(LogLevel.Debug))
            logger.LogDebug("[EventStore] Created table `{tableName}` in keyspace `{keyspace}`...", PartionsTableName, session.Keyspace);
    }

    class CreateTablePreparedStatement : PreparedStatementCache
    {
        public CreateTablePreparedStatement(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider) { }

        internal override string GetQueryTemplate() => CreateProjectionPartionsTableTemplate;
    }
}
