using System;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStoreInitializer : IInitializableProjectionStore
    {
        static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraProjectionStoreInitializer));

        private readonly IProjectionStoreStorageManager projectionsSchema;
        private readonly CassandraSnapshotStoreSchema snapshotsSchema;
        private readonly IProjectionsNamingStrategy naming;

        public CassandraProjectionStoreInitializer(IProjectionStoreStorageManager projectionsSchema, CassandraSnapshotStoreSchema snapshotsSchema, IProjectionsNamingStrategy naming)
        {
            if (projectionsSchema is null) throw new ArgumentNullException(nameof(projectionsSchema));
            if (snapshotsSchema is null) throw new ArgumentNullException(nameof(snapshotsSchema));

            this.naming = naming;
            this.projectionsSchema = projectionsSchema;
            this.snapshotsSchema = snapshotsSchema;
        }

        public void Initialize(ProjectionVersion version)
        {
            string projectionColumnFamily = naming.GetColumnFamily(version);
            logger.Debug(() => $"[Projection Store] Initializing projection store with column family `{projectionColumnFamily}`...");
            projectionsSchema.CreateProjectionsStorage(projectionColumnFamily);
            logger.Debug(() => $"[Projection Store] Initialized projection store with column family `{projectionColumnFamily}`");

            string snapshotColumnFamily = naming.GetSnapshotColumnFamily(version);
            logger.Debug(() => $"[Snapshot Store] Initializing snapshot store with column family `{snapshotColumnFamily}`....");
            snapshotsSchema.CreateTable(snapshotColumnFamily);
            logger.Debug(() => $"[Snapshot Store] Initialized snapshot store with column family `{snapshotColumnFamily}`");
        }
    }
}
