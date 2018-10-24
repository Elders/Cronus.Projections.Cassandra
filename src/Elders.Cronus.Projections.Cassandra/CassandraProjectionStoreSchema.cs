using System;
using System.Collections.Concurrent;
using System.Linq;
using Cassandra;
using Elders.Cronus.AtomicAction;
using Elders.Cronus.Projections.Cassandra.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStoreSchema
    {
        static ILog log = LogProvider.GetLogger(typeof(CassandraProjectionStoreSchema));

        const string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, sm int, evarid text, evarrev int, evarts bigint, evarpos int, data blob, PRIMARY KEY ((id, sm), evarid, evarrev, evarpos, evarts)) WITH CLUSTERING ORDER BY (evarid ASC);";
        const string DropQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";

        readonly ISession sessionForSchemaChanges;
        readonly ILock @lock;
        private readonly TimeSpan lockTtl;
        readonly ConcurrentDictionary<string, PreparedStatement> CreatePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> DropPreparedStatements;

        /// <summary>
        /// Used for cassandra schema changes exclusively
        /// https://issues.apache.org/jira/browse/CASSANDRA-10699
        /// https://issues.apache.org/jira/browse/CASSANDRA-11429
        /// </summary>
        /// <param name="sessionForSchemaChanges"></param>
        public CassandraProjectionStoreSchema(ICassandraProvider cassandraProvider, ILock @lock)
        {
            if (ReferenceEquals(null, cassandraProvider)) throw new ArgumentNullException(nameof(cassandraProvider));
            if (ReferenceEquals(null, @lock)) throw new ArgumentNullException(nameof(@lock));

            this.sessionForSchemaChanges = cassandraProvider.GetSchemaSession();
            this.@lock = @lock;
            this.lockTtl = TimeSpan.FromSeconds(2);
            if (lockTtl == TimeSpan.Zero) throw new ArgumentException("Lock ttl must be more than 0", nameof(lockTtl));
            CreatePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            DropPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public void DropTable(string location)
        {
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
                log.Debug($"[Projections] Could not acquire lock for `{location}` to drop projections table");
            }
        }

        public void CreateTable(string location)
        {
            if (@lock.Lock(location, lockTtl))
            {
                try
                {
                    log.Debug(() => $"[Projections] Creating table `{location}` with `{sessionForSchemaChanges.Cluster.AllHosts().First().Address}`...");
                    var statement = CreatePreparedStatements.GetOrAdd(location, x => BuildCreatePreparedStatement(CreateProjectionEventsTableTemplate, x));
                    statement.SetConsistencyLevel(ConsistencyLevel.All);
                    sessionForSchemaChanges.Execute(statement.Bind());
                    log.Debug(() => $"[Projections] Created table `{location}`... Maybe?!");
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
                log.Warn($"[Projections] Could not acquire lock for `{location}` to create projections table");
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

        public void CreateProjectionsStorage(string location)
        {
            CreateTable(location);
        }
    }
}
