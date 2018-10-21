using System;
using System.Collections.Concurrent;
using System.Linq;
using Cassandra;
using DataStaxCassandra = Cassandra;
using Elders.Cronus.AtomicAction;
using Elders.Cronus.Projections.Cassandra.Config;
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

        readonly ILock @lock;
        private readonly TimeSpan lockTtl;
        readonly ISession sessionForSchemaChanges;
        readonly ConcurrentDictionary<string, PreparedStatement> CreatePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> DropPreparedStatements;

        public CassandraSnapshotStoreSchema(CassandraProvider cassandraProvider, ILock @lock)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            if (@lock is null) throw new ArgumentNullException(nameof(@lock));


            this.sessionForSchemaChanges = cassandraProvider.GetLiveSchemaSession();
            this.@lock = @lock;
            this.lockTtl = TimeSpan.FromSeconds(2);
            if (lockTtl == TimeSpan.Zero) throw new ArgumentException("Lock ttl must be more than 0", nameof(lockTtl));
            CreatePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            DropPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        private static ISession GetLiveSchemaSession(CassandraProvider cassandraProvider)
        {
            var hosts = cassandraProvider.GetCluster().AllHosts().ToList();
            ISession schemaSession = null;
            var counter = 0;

            while (ReferenceEquals(null, schemaSession))
            {
                var schemaCreatorVoltron = hosts.ElementAtOrDefault(counter++);
                if (ReferenceEquals(null, schemaCreatorVoltron))
                    throw new InvalidOperationException($"Could not find a Cassandra node! Hosts: '{string.Join(", ", hosts.Select(x => x.Address))}'");

                var schemaCluster = DataStaxCassandra.Cluster
                    .Builder()
                    .WithReconnectionPolicy(new DataStaxCassandra.ExponentialReconnectionPolicy(100, 100000))
                    .WithRetryPolicy(new NoHintedHandOffRetryPolicy())
                    .AddContactPoint(schemaCreatorVoltron.Address)
                    .Build();

                try
                {
                    schemaSession = schemaCluster.Connect(cassandraProvider.Keyspace);
                }
                catch (DataStaxCassandra.NoHostAvailableException)
                {
                    if (counter < hosts.Count)
                        continue;
                    else
                        throw;
                }
            }

            return schemaSession;
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
                log.Info($"[Projections] Could not acquire lock for `{location}` to drop snapshots table");
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
                    log.Info(() => $"Creating snapshot table `{location}` with `{sessionForSchemaChanges.Cluster.AllHosts().First().Address}`...");
                    var statement = CreatePreparedStatements.GetOrAdd(location, x => BuildCreatePreparedStatement(CreateSnapshopEventsTableTemplate, x));
                    statement.SetConsistencyLevel(ConsistencyLevel.All);
                    sessionForSchemaChanges.Execute(statement.Bind());
                    log.Info(() => $"Created snapshot table `{location}`... Maybe?!");
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
                log.Info($"[Projections] Could not acquire lock for `{location}` to create snapshots table");
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
