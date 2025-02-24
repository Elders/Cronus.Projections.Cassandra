using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    [Obsolete("Will be removed in v12. Use CassandraProjectionStoreSchemaNew instead.")]
    public class CassandraProjectionStoreSchema : IProjectionStoreStorageManager
    {
        private readonly ConcurrentDictionary<string, bool> initializedLocations;

        private readonly ILogger<CassandraProjectionStoreSchema> logger;
        private readonly ICassandraProvider cassandraProvider;

        const string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"".""{1}"" (id blob, data blob, ts bigint, PRIMARY KEY (id, ts)) WITH CLUSTERING ORDER BY (ts ASC);";
        const string DropQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";

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
        public CassandraProjectionStoreSchema(ICassandraProvider cassandraProvider, ILogger<CassandraProjectionStoreSchema> logger)
        {
            if (ReferenceEquals(null, cassandraProvider)) throw new ArgumentNullException(nameof(cassandraProvider));
            this.cassandraProvider = cassandraProvider;
            this.logger = logger;

            initializedLocations = new ConcurrentDictionary<string, bool>();
        }

        public async Task DropTableAsync(string location)
        {
            if (string.IsNullOrWhiteSpace(location)) throw new ArgumentNullException(nameof(location));

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            string query = string.Format(DropQueryTemplate, location);
            PreparedStatement statement = await session.PrepareAsync(query).ConfigureAwait(false);
            statement = statement.SetConsistencyLevel(ConsistencyLevel.All);
            await session.ExecuteAsync(statement.Bind()).ConfigureAwait(false);
        }

        public async Task CreateTableAsync(string location)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[Projections] Creating table `{tableName}` with `{address}`...", location, session.Cluster.AllHosts().First().Address);
            string query = string.Format(CreateProjectionEventsTableTemplate, session.Keyspace, location);
            PreparedStatement statement = await session.PrepareAsync(query).ConfigureAwait(false);
            statement = statement.SetConsistencyLevel(ConsistencyLevel.All);
            await session.ExecuteAsync(statement.Bind()).ConfigureAwait(false);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[Projections] Created table `{tableName}`... Maybe?!", location);
        }

        public async Task CreateProjectionsStorageAsync(string location)
        {
            if (initializedLocations.TryGetValue(location, out bool isInitialized))
            {
                if (isInitialized == false)
                {
                    await CreateTableAsync(location).ConfigureAwait(false);
                    initializedLocations.TryUpdate(location, true, false);
                }
            }
            else
            {
                await CreateTableAsync(location).ConfigureAwait(false);
                initializedLocations.TryAdd(location, true);
            }
        }
    }
}
