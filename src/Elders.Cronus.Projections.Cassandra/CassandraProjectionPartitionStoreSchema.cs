using Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Elders.Cronus.Projections.Cassandra
{
    public interface ICassandraProjectionPartitionStoreSchema
    {
        Task CreateProjectionPartitionsStorage();
    }

    public class CassandraProjectionPartitionStoreSchema : ICassandraProjectionPartitionStoreSchema
    {
        private readonly ILogger<CassandraProjectionPartitionStoreSchema> logger;
        private readonly ICassandraProvider cassandraProvider;

        const string CreateProjectionPartionsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (pt text, id blob, pid bigint, PRIMARY KEY ((pt,id), pid)) WITH CLUSTERING ORDER BY (pid ASC)";
        const string PartionsTableName = "projection_partitions";

        public CassandraProjectionPartitionStoreSchema(ICassandraProvider cassandraProvider, ILogger<CassandraProjectionPartitionStoreSchema> logger)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
            this.logger = logger;
        }

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();

        public async Task CreateProjectionPartitionsStorage()
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("[EventStore] Creating table `{tableName}` with `{address}` in keyspace `{keyspace}`...", PartionsTableName, session.Cluster.AllHosts().First().Address, session.Keyspace);

                PreparedStatement createEventsTableStatement = await session.PrepareAsync(string.Format(CreateProjectionPartionsTableTemplate, PartionsTableName).ToLower()).ConfigureAwait(false);
                createEventsTableStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                await session.ExecuteAsync(createEventsTableStatement.Bind()).ConfigureAwait(false);

                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("[EventStore] Created table `{tableName}` in keyspace `{keyspace}`...", PartionsTableName, session.Keyspace);
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
