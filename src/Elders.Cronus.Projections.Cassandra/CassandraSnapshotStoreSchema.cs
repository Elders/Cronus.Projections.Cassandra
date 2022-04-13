using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public sealed class CassandraSnapshotStoreSchema
    {
        private readonly ILogger<CassandraSnapshotStoreSchema> logger;
        private readonly ICassandraProvider cassandraProvider;

        const string CreateSnapshopEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, rev int, data blob, PRIMARY KEY (id, rev)) WITH CLUSTERING ORDER BY (rev DESC);";
        const string DropQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();
        public async Task<string> GetKeypaceAsync()
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            return session.Keyspace;
        }

        public CassandraSnapshotStoreSchema(ICassandraProvider cassandraProvider, ILogger<CassandraSnapshotStoreSchema> logger)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            this.cassandraProvider = cassandraProvider;
            this.logger = logger;
        }

        public async Task DropTableAsync(string location)
        {
            if (string.IsNullOrWhiteSpace(location)) throw new ArgumentNullException(nameof(location));

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            string query = string.Format(DropQueryTemplate, location);
            PreparedStatement statement = await session.PrepareAsync(query).ConfigureAwait(false);
            statement.SetConsistencyLevel(ConsistencyLevel.All);
            await session.ExecuteAsync(statement.Bind()).ConfigureAwait(false);
        }

        public async Task CreateTableAsync(string location)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(location)) throw new ArgumentNullException(nameof(location));
            logger.Debug(() => $"[Projections] Creating snapshot table `{location}` with `{session.Cluster.AllHosts().First().Address}`...");
            string query = string.Format(CreateSnapshopEventsTableTemplate, location);
            PreparedStatement statement = await session.PrepareAsync(query).ConfigureAwait(false);
            statement.SetConsistencyLevel(ConsistencyLevel.All);
            await session.ExecuteAsync(statement.Bind()).ConfigureAwait(false);
            logger.Debug(() => $"[Projections] Created snapshot table `{location}`... Maybe?!");
        }
    }
}
