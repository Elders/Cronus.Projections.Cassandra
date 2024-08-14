using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStoreInitializer : IInitializableProjectionStore
    {
        static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraProjectionStoreInitializer));

        private readonly IProjectionStoreStorageManager projectionsSchemaLegacy;
        private readonly ICassandraProjectionPartitionStoreSchema partitionsSchema;
        private readonly ICassandraProjectionStoreSchemaNew projectionsSchemaNew;
        private readonly VersionedProjectionsNaming naming;

        public CassandraProjectionStoreInitializer(IProjectionStoreStorageManager projectionsSchema, ICassandraProjectionPartitionStoreSchema partitionsSchema, VersionedProjectionsNaming naming, ICassandraProjectionStoreSchemaNew projectionsSchemaNew)
        {
            if (projectionsSchema is null) throw new ArgumentNullException(nameof(projectionsSchema));

            this.naming = naming;
            this.partitionsSchema = partitionsSchema;
            this.projectionsSchemaLegacy = projectionsSchema;
            this.projectionsSchemaNew = projectionsSchemaNew;
        }

        public async Task<bool> InitializeAsync(ProjectionVersion version)
        {
            try
            {
                await partitionsSchema.CreateProjectionPartitionsStorage(); // partitions

                string projectionColumnFamily = naming.GetColumnFamily(version);
                logger.Debug(() => $"[Projection Store] Initializing projection store with column family `{projectionColumnFamily}`..."); // old store
                Task createProjectionStorageTask = projectionsSchemaLegacy.CreateProjectionsStorageAsync(projectionColumnFamily);
                await createProjectionStorageTask.ConfigureAwait(false);
                logger.Debug(() => $"[Projection Store] Initialized projection store with column family `{projectionColumnFamily}`");

                string projectionColumnFamilyNew = naming.GetColumnFamilyNew(version);
                logger.Debug(() => $"[Projection Store] Initializing projection store with column family `{projectionColumnFamilyNew}`..."); // new store
                Task createProjectionStorageTaskNew = projectionsSchemaNew.CreateProjectionStorageNewAsync(projectionColumnFamilyNew);
                await createProjectionStorageTaskNew.ConfigureAwait(false);
                logger.Debug(() => $"[Projection Store] Initialized projection store with column family `{projectionColumnFamilyNew}`");

                return createProjectionStorageTask.IsCompletedSuccessfully && createProjectionStorageTaskNew.IsCompletedSuccessfully;
            }
            catch (Exception ex) when (logger.ErrorException(ex, () => $"Failed to initialize projection version {version}"))
            {
                return false;
            }
        }
    }
}
