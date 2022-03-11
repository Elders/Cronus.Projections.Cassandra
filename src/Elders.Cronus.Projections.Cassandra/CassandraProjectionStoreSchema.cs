using System;
using System.Linq;
using Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStoreSchema : IProjectionStoreStorageManager
    {
        static ILogger logger = CronusLogger.CreateLogger(typeof(CassandraProjectionStoreSchema));

        const string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, sm int, evarid text, evarrev int, evarts bigint, evarpos int, data blob, PRIMARY KEY ((id, sm), evarts, evarid, evarrev, evarpos)) WITH CLUSTERING ORDER BY (evarts ASC);";
        const string DropQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";

        readonly ISession sessionForSchemaChanges;

        /// <summary>
        /// Used for cassandra schema changes exclusively
        /// https://issues.apache.org/jira/browse/CASSANDRA-10699
        /// https://issues.apache.org/jira/browse/CASSANDRA-11429
        /// </summary>
        /// <param name="sessionForSchemaChanges"></param>
        public CassandraProjectionStoreSchema(ICassandraProvider cassandraProvider)
        {
            if (ReferenceEquals(null, cassandraProvider)) throw new ArgumentNullException(nameof(cassandraProvider));

            this.sessionForSchemaChanges = cassandraProvider.GetSession();
        }

        public string Keyspace => sessionForSchemaChanges.Keyspace;

        public void DropTable(string location)
        {
            var query = string.Format(DropQueryTemplate, location);
            sessionForSchemaChanges.Execute(query, ConsistencyLevel.All);
        }

        public void CreateTable(string location)
        {
            logger.Debug(() => $"[Projections] Creating table `{location}` with `{sessionForSchemaChanges.Cluster.AllHosts().First().Address}`...");
            var query = string.Format(CreateProjectionEventsTableTemplate, location);
            var result = sessionForSchemaChanges.Execute(query, ConsistencyLevel.All);
            logger.Debug(() => $"[Projections] Created table `{location}`... Maybe?!");
        }

        public void CreateProjectionsStorage(string location)
        {
            CreateTable(location);
        }
    }
}
