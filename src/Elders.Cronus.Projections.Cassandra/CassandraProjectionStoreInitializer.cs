using System;
using System.Threading.Tasks;
using Elders.Cronus.AtomicAction;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStoreInitializer : IInitializableProjectionStore
    {
        static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraProjectionStoreInitializer));

        private readonly IProjectionStoreStorageManager projectionsSchema;
        private readonly CassandraSnapshotStoreSchema snapshotsSchema;
        private readonly VersionedProjectionsNaming naming;
        private readonly ILock @lock;
        private readonly TimeSpan lockTtl;

        public CassandraProjectionStoreInitializer(IProjectionStoreStorageManager projectionsSchema, CassandraSnapshotStoreSchema snapshotsSchema, VersionedProjectionsNaming naming, ILock @lock)
        {
            if (projectionsSchema is null) throw new ArgumentNullException(nameof(projectionsSchema));
            if (snapshotsSchema is null) throw new ArgumentNullException(nameof(snapshotsSchema));

            this.naming = naming;
            this.projectionsSchema = projectionsSchema;
            this.snapshotsSchema = snapshotsSchema;
            this.@lock = @lock;

            this.lockTtl = TimeSpan.FromSeconds(2);
            if (lockTtl == TimeSpan.Zero) throw new ArgumentException("Lock ttl must be more than 0", nameof(lockTtl));
        }

        public async Task<bool> InitializeAsync(ProjectionVersion version)
        {
            try
            {
                string projectionColumnFamily = naming.GetColumnFamily(version);
                string snapshotColumnFamily = naming.GetSnapshotColumnFamily(version);

                logger.Debug(() => $"[Projection Store] Initializing projection store with column family `{projectionColumnFamily}` and `{snapshotColumnFamily}`...");
                Task createProjectionStorageTask = projectionsSchema.CreateProjectionsStorageAsync(projectionColumnFamily);
                Task createProjectionSnapshotStorageTask = snapshotsSchema.CreateSnapshotStorageAsync(snapshotColumnFamily);
                logger.Debug(() => $"[Projection Store] Initialized projection store with column family `{projectionColumnFamily}` and `{snapshotColumnFamily}`");

                await createProjectionStorageTask.ConfigureAwait(false);
                await createProjectionSnapshotStorageTask.ConfigureAwait(false);

                return createProjectionStorageTask.IsCompletedSuccessfully && createProjectionSnapshotStorageTask.IsCompletedSuccessfully;
            }
            catch (Exception ex) when (logger.ErrorException(ex, () => $"Failed to initialize projection version {version}"))
            {
                return false;
            }
        }
    }
}
