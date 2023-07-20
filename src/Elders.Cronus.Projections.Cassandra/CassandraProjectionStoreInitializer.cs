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
        private readonly ICassandraSnapshotStoreSchema snapshotsSchema;
        private readonly VersionedProjectionsNaming naming;

        public CassandraProjectionStoreInitializer(IProjectionStoreStorageManager projectionsSchema, ICassandraSnapshotStoreSchema snapshotsSchema, VersionedProjectionsNaming naming)
        {
            if (projectionsSchema is null) throw new ArgumentNullException(nameof(projectionsSchema));
            if (snapshotsSchema is null) throw new ArgumentNullException(nameof(snapshotsSchema));

            this.naming = naming;
            this.projectionsSchema = projectionsSchema;
            this.snapshotsSchema = snapshotsSchema;
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
