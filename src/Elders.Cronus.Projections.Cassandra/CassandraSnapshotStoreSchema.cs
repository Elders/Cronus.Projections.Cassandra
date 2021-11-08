using System;
using System.Linq;
using Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public sealed class CassandraSnapshotStoreSchema
    {
        static ILogger logger = CronusLogger.CreateLogger(typeof(CassandraSnapshotStoreSchema));

        const string CreateSnapshopEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, rev int, data blob, PRIMARY KEY (id, rev)) WITH CLUSTERING ORDER BY (rev DESC);";
        const string DropQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";

        readonly ISession sessionForSchemaChanges;

        public CassandraSnapshotStoreSchema(ICassandraProvider cassandraProvider)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.sessionForSchemaChanges = cassandraProvider.GetSession();
        }

        public string Keyspace => sessionForSchemaChanges.Keyspace;

        public void DropTable(string location)
        {
            if (string.IsNullOrWhiteSpace(location)) throw new ArgumentNullException(nameof(location));

            var query = string.Format(DropQueryTemplate, location);
            sessionForSchemaChanges.Execute(query, ConsistencyLevel.All);
        }

        public void CreateTable(string location)
        {
            if (string.IsNullOrWhiteSpace(location)) throw new ArgumentNullException(nameof(location));

            logger.Debug(() => $"[Projections] Creating snapshot table `{location}` with `{sessionForSchemaChanges.Cluster.AllHosts().First().Address}`...");
            var query = string.Format(CreateSnapshopEventsTableTemplate, location);
            sessionForSchemaChanges.Execute(query, ConsistencyLevel.All);
            logger.Debug(() => $"[Projections] Created snapshot table `{location}`... Maybe?!");
        }
    }
}
