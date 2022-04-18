using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStoreSchema : IProjectionStoreStorageManager
    {
        private readonly ILogger<CassandraProjectionStoreSchema> logger;
        private readonly ICassandraProvider cassandraProvider;

        const string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, sm int, evarid text, evarrev int, evarts bigint, evarpos int, data blob, PRIMARY KEY ((id, sm), evarts, evarid, evarrev, evarpos)) WITH CLUSTERING ORDER BY (evarts ASC);";
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
            logger.Debug(() => $"[Projections] Creating table `{location}` with `{session.Cluster.AllHosts().First().Address}`...");
            string query = string.Format(CreateProjectionEventsTableTemplate, location);
            PreparedStatement statement = await session.PrepareAsync(query).ConfigureAwait(false);
            statement = statement.SetConsistencyLevel(ConsistencyLevel.All);
            await session.ExecuteAsync(statement.Bind()).ConfigureAwait(false);
            logger.Debug(() => $"[Projections] Created table `{location}`... Maybe?!");
        }

        private static SemaphoreSlim threadGate = new SemaphoreSlim(1);
        private static KeyValuePair<string, bool> initializedLocation = new KeyValuePair<string, bool>();
        private static int held = 0;

        public async Task CreateProjectionsStorageAsync(string location)
        {
            if (initializedLocation.Key is not null && initializedLocation.Key.Equals(location, StringComparison.OrdinalIgnoreCase) == false)
                initializedLocation = new KeyValuePair<string, bool>(location, false);

            held++;
            await threadGate.WaitAsync().ConfigureAwait(false);
            if (initializedLocation.Value == false)
            {
                try
                {
                    await CreateTableAsync(location).ConfigureAwait(false);
                    initializedLocation = new KeyValuePair<string, bool>(location, true);
                }
                catch (Exception ex)
                {
                    logger.Debug(() => $"Failed to initialize table {location}. {ex}");
                    initializedLocation = new KeyValuePair<string, bool>(location, false);
                }
            }
            threadGate?.Release(held);
        }
    }
}
