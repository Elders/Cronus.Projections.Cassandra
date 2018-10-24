using System;
using System.Collections.Concurrent;
using System.Linq;
using Cassandra;
using Elders.Cronus.AtomicAction;
using Elders.Cronus.Projections.Cassandra.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public sealed class CassandraSnapshotStoreSchema
    {
        static ILog log = LogProvider.GetLogger(typeof(CassandraSnapshotStoreSchema));

        static readonly object createMutex = new object();
        static readonly object dropMutex = new object();

        const string CreateSnapshopEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, rev int, data blob, PRIMARY KEY (id, rev)) WITH CLUSTERING ORDER BY (rev DESC);";
        const string DropQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";

        readonly ILock @lock;
        private readonly TimeSpan lockTtl;
        readonly ISession sessionForSchemaChanges;
        readonly ConcurrentDictionary<string, PreparedStatement> CreatePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> DropPreparedStatements;

        public CassandraSnapshotStoreSchema(ICassandraProvider cassandraProvider, ILock @lock)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            if (@lock is null) throw new ArgumentNullException(nameof(@lock));


            this.sessionForSchemaChanges = cassandraProvider.GetSchemaSession();
            this.@lock = @lock;
            this.lockTtl = TimeSpan.FromSeconds(2);
            if (lockTtl == TimeSpan.Zero) throw new ArgumentException("Lock ttl must be more than 0", nameof(lockTtl));
            CreatePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            DropPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public void DropTable(string location)
        {
            if (string.IsNullOrWhiteSpace(location)) throw new ArgumentNullException(nameof(location));

            // https://issues.apache.org/jira/browse/CASSANDRA-10699
            // https://issues.apache.org/jira/browse/CASSANDRA-11429
            if (@lock.Lock(location, lockTtl))
            {
                try
                {
                    var statement = CreatePreparedStatements.GetOrAdd(location, x => BuildDropPreparedStatemnt(x));
                    statement.SetConsistencyLevel(ConsistencyLevel.All);
                    sessionForSchemaChanges.Execute(statement.Bind());
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    @lock.Unlock(location);
                }
            }
            else
            {
                log.Warn($"[Projections] Could not acquire lock for `{location}` to drop snapshots table");
            }
        }

        public void CreateTable(string location)
        {
            if (string.IsNullOrWhiteSpace(location)) throw new ArgumentNullException(nameof(location));

            // https://issues.apache.org/jira/browse/CASSANDRA-10699
            // https://issues.apache.org/jira/browse/CASSANDRA-11429
            if (@lock.Lock(location, lockTtl))
            {
                try
                {
                    log.Debug(() => $"[Projections] Creating snapshot table `{location}` with `{sessionForSchemaChanges.Cluster.AllHosts().First().Address}`...");
                    var statement = CreatePreparedStatements.GetOrAdd(location, x => BuildCreatePreparedStatement(CreateSnapshopEventsTableTemplate, x));
                    statement.SetConsistencyLevel(ConsistencyLevel.All);
                    sessionForSchemaChanges.Execute(statement.Bind());
                    log.Debug(() => $"[Projections] Created snapshot table `{location}`... Maybe?!");
                }
                catch (Exception)
                {
                    throw;
                }
                finally
                {
                    @lock.Unlock(location);
                }
            }
            else
            {
                log.Warn($"[Projections] Could not acquire lock for `{location}` to create snapshots table");
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
