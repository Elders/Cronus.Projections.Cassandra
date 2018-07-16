using System;
using System.Collections.Concurrent;
using System.Linq;
using Cassandra;
using Elders.Cronus.Projections.Cassandra.Logging;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public class CassandraSnapshotStoreSchema
    {
        static ILog log = LogProvider.GetLogger(typeof(CassandraSnapshotStoreSchema));

        static readonly object createMutex = new object();
        static readonly object dropMutex = new object();

        const string CreateSnapshopEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, rev int, data blob, PRIMARY KEY (id, rev)) WITH CLUSTERING ORDER BY (rev DESC);";
        const string DropQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";

        readonly ISession sessionForSchemaChanges;
        readonly ConcurrentDictionary<string, PreparedStatement> CreatePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> DropPreparedStatements;

        public CassandraSnapshotStoreSchema(ISession sessionForSchemaChanges)
        {
            if (sessionForSchemaChanges is null) throw new ArgumentNullException();

            this.sessionForSchemaChanges = sessionForSchemaChanges;

            CreatePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            DropPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public void DropTable(string location)
        {
            if (string.IsNullOrWhiteSpace(location)) throw new ArgumentNullException(nameof(location));

            // https://issues.apache.org/jira/browse/CASSANDRA-10699
            // https://issues.apache.org/jira/browse/CASSANDRA-11429
            lock (dropMutex)
            {
                var statement = CreatePreparedStatements.GetOrAdd(location, x => BuildDropPreparedStatemnt(x));
                statement.SetConsistencyLevel(ConsistencyLevel.All);
                sessionForSchemaChanges.Execute(statement.Bind());
            }
        }

        public void CreateTable(string location)
        {
            if (string.IsNullOrWhiteSpace(location)) throw new ArgumentNullException(nameof(location));

            CreateTable(CreateSnapshopEventsTableTemplate, location);
        }

        void CreateTable(string template, string location)
        {
            // https://issues.apache.org/jira/browse/CASSANDRA-10699
            // https://issues.apache.org/jira/browse/CASSANDRA-11429
            lock (createMutex)
            {
                log.Info(() => $"Creating snapshot table `{location}` with `{sessionForSchemaChanges.Cluster.AllHosts().First().Address}`...");
                var statement = CreatePreparedStatements.GetOrAdd(location, x => BuildCreatePreparedStatement(template, x));
                statement.SetConsistencyLevel(ConsistencyLevel.All);
                sessionForSchemaChanges.Execute(statement.Bind());
                log.Info(() => $"Created snapshot table `{location}`... Maybe?!");
            }
        }

        PreparedStatement BuildDropPreparedStatemnt(string columnFamily)
        {
            return sessionForSchemaChanges.Prepare(string.Format(DropQueryTemplate, columnFamily));
        }

        PreparedStatement BuildCreatePreparedStatement(string template, string columnFamily)
        {
            return sessionForSchemaChanges.Prepare(string.Format(template, columnFamily));
        }
    }
}
