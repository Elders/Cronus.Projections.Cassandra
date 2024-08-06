using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public interface IInitializableProjectionPartionsStore
    {
        Task<bool> InitializeAsync();
    }

    public class CassandraProjectionPartionsStoreInitializer : IInitializableProjectionPartionsStore
    {
        static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraProjectionPartionsStoreInitializer));

        private readonly ICassandraProjectionPartitionStoreSchema projectionPartitionsSchema;

        public CassandraProjectionPartionsStoreInitializer(ICassandraProjectionPartitionStoreSchema projectionPartitionsSchema)
        {
            if (projectionPartitionsSchema is null) throw new ArgumentNullException(nameof(projectionPartitionsSchema));

            this.projectionPartitionsSchema = projectionPartitionsSchema;
        }

        public async Task<bool> InitializeAsync()
        {
            try
            {
                logger.Debug(() => $"[Projection Store] Initializing projection partitions store...");
                Task createProjectionStorageTask = projectionPartitionsSchema.CreateProjectionPartitionsStorage();
                await createProjectionStorageTask.ConfigureAwait(false);
                logger.Debug(() => $"[Projection Store] Initialized projection store");

                return createProjectionStorageTask.IsCompletedSuccessfully;
            }
            catch (Exception ex) when (logger.ErrorException(ex, () => $"Failed to initialize projection partitions"))
            {
                return false;
            }
        }
    }


    public class CassandraProjectionStoreInitializer : IInitializableProjectionStore
    {
        static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraProjectionStoreInitializer));

        private readonly IProjectionStoreStorageManager projectionsSchema;
        private readonly IInitializableProjectionPartionsStore initializableProjectionStore;
        private readonly VersionedProjectionsNaming naming;

        public CassandraProjectionStoreInitializer(IProjectionStoreStorageManager projectionsSchema, IInitializableProjectionPartionsStore initializableProjectionStore, VersionedProjectionsNaming naming)
        {
            if (projectionsSchema is null) throw new ArgumentNullException(nameof(projectionsSchema));

            this.naming = naming;
            this.initializableProjectionStore = initializableProjectionStore;
            this.projectionsSchema = projectionsSchema;
        }

        public async Task<bool> InitializeAsync(ProjectionVersion version)
        {
            try
            {
                await initializableProjectionStore.InitializeAsync();

                string projectionColumnFamily = naming.GetColumnFamily(version);

                logger.Debug(() => $"[Projection Store] Initializing projection store with column family `{projectionColumnFamily}`...");
                Task createProjectionStorageTask = projectionsSchema.CreateProjectionsStorageAsync(projectionColumnFamily);
                await createProjectionStorageTask.ConfigureAwait(false);
                logger.Debug(() => $"[Projection Store] Initialized projection store with column family `{projectionColumnFamily}`");

                return createProjectionStorageTask.IsCompletedSuccessfully;
            }
            catch (Exception ex) when (logger.ErrorException(ex, () => $"Failed to initialize projection version {version}"))
            {
                return false;
            }
        }
    }
}
