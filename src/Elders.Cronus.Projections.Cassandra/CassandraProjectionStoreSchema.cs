using System;
using System.Linq;
using Cassandra;
using Elders.Cronus.AtomicAction;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStoreSchema : IProjectionStoreStorageManager
    {
        static ILogger logger = CronusLogger.CreateLogger(typeof(CassandraProjectionStoreSchema));

        const string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, sm int, evarid text, evarrev int, evarts bigint, evarpos int, data blob, PRIMARY KEY ((id, sm), evarid, evarrev, evarpos, evarts)) WITH CLUSTERING ORDER BY (evarts ASC);";
        const string DropQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";

        readonly ISession sessionForSchemaChanges;
        readonly ILock @lock;
        private readonly TimeSpan lockTtl;

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

            this.sessionForSchemaChanges = cassandraProvider.GetSession();
            this.@lock = @lock;
            this.lockTtl = TimeSpan.FromSeconds(2);
            if (lockTtl == TimeSpan.Zero) throw new ArgumentException("Lock ttl must be more than 0", nameof(lockTtl));
        }

        public string Keyspace => sessionForSchemaChanges.Keyspace;

        public void DropTable(string location)
        {
            if (@lock.Lock(location, lockTtl))
            {
                try
                {
                    var query = string.Format(DropQueryTemplate, location);
                    sessionForSchemaChanges.Execute(query, ConsistencyLevel.All);
                }
                finally
                {
                    @lock.Unlock(location);
                }
            }
            else
            {
                logger.Debug($"[Projections] Could not acquire lock for `{location}` to drop projections table");
            }
        }

        public void CreateTable(string location)
        {
            if (@lock.Lock(location, lockTtl))
            {
                try
                {
                    logger.Debug(() => $"[Projections] Creating table `{location}` with `{sessionForSchemaChanges.Cluster.AllHosts().First().Address}`...");
                    var query = string.Format(CreateProjectionEventsTableTemplate, location);
                    var result = sessionForSchemaChanges.Execute(query, ConsistencyLevel.All);
                    logger.Debug(() => $"[Projections] Created table `{location}`... Maybe?!");
                }
                finally
                {
                    @lock.Unlock(location);
                }
            }
            else
            {
                logger.Warn($"[Projections] Could not acquire lock for `{location}` to create projections table");
            }
        }

        public void CreateProjectionsStorage(string location)
        {
            CreateTable(location);
        }
    }
}
