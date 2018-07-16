using Cassandra;
using System.Collections.Concurrent;
using System.Linq;
using Elders.Cronus.Projections.Cassandra.Logging;
using System;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class CassandraProjectionStoreSchema
    {
        static ILog log = LogProvider.GetLogger(typeof(CassandraProjectionStoreSchema));

        static readonly object createMutex = new object();
        static readonly object dropMutex = new object();

        const string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, sm int, evarid text, evarrev int, evarts bigint, evarpos int, data blob, PRIMARY KEY ((id, sm), evarid, evarrev, evarpos, evarts)) WITH CLUSTERING ORDER BY (evarid ASC);";
        const string DropQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";

        readonly ISession sessionForSchemaChanges;
        readonly ConcurrentDictionary<string, PreparedStatement> CreatePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> DropPreparedStatements;

        /// <summary>
        /// Used for cassandra schema changes exclusively
        /// https://issues.apache.org/jira/browse/CASSANDRA-10699
        /// https://issues.apache.org/jira/browse/CASSANDRA-11429
        /// </summary>
        /// <param name="sessionForSchemaChanges"></param>
        public CassandraProjectionStoreSchema(ISession sessionForSchemaChanges)
        {
            if (ReferenceEquals(null, sessionForSchemaChanges)) throw new ArgumentNullException(nameof(sessionForSchemaChanges));

            this.sessionForSchemaChanges = sessionForSchemaChanges;

            CreatePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            DropPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public void DropTable(string location)
        {
            lock (dropMutex)
            {
                var statement = CreatePreparedStatements.GetOrAdd(location, x => BuildDropPreparedStatemnt(x));
                statement.SetConsistencyLevel(ConsistencyLevel.All);
                sessionForSchemaChanges.Execute(statement.Bind());
            }
        }

        public void CreateTable(string location)
        {
            CreateTable(CreateProjectionEventsTableTemplate, location);
        }


        void CreateTable(string template, string location)
        {
            lock (createMutex)
            {
                log.Info(() => $"Creating table `{location}` with `{sessionForSchemaChanges.Cluster.AllHosts().First().Address}`...");
                var statement = CreatePreparedStatements.GetOrAdd(location, x => BuildCreatePreparedStatement(template, x));
                statement.SetConsistencyLevel(ConsistencyLevel.All);
                sessionForSchemaChanges.Execute(statement.Bind());
                log.Info(() => $"Created table `{location}`... Maybe?!");
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
