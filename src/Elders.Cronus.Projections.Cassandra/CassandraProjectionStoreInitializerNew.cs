using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra;

public sealed class CassandraProjectionStoreInitializerNew : IInitializableProjectionStore
{
    static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraProjectionStoreInitializerNew));
    private readonly CassandraProjectionStoreInitializer legacyInitializer;
    private readonly ICassandraProjectionPartitionStoreSchema partitionsSchema;
    private readonly ICassandraProjectionStoreSchemaNew projectionsSchemaNew;
    private readonly VersionedProjectionsNaming naming;

    public CassandraProjectionStoreInitializerNew(CassandraProjectionStoreInitializer legacyInitializer, ICassandraProjectionPartitionStoreSchema partitionsSchema, VersionedProjectionsNaming naming, ICassandraProjectionStoreSchemaNew projectionsSchemaNew)
    {
        this.naming = naming;
        this.partitionsSchema = partitionsSchema;
        this.legacyInitializer = legacyInitializer;
        this.projectionsSchemaNew = projectionsSchemaNew;
    }

    public async Task<bool> InitializeAsync(ProjectionVersion version)
    {
        try
        {
            await legacyInitializer.InitializeAsync(version).ConfigureAwait(false); //legacy store

            await partitionsSchema.CreateProjectionPartitionsStorage(); // partitions

            string projectionColumnFamilyNew = naming.GetColumnFamilyNew(version);
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[Projection Store] Initializing projection store with column family `{projectionColumnFamilyNew}`...", projectionColumnFamilyNew); // new store
            Task createProjectionStorageTaskNew = projectionsSchemaNew.CreateProjectionStorageNewAsync(projectionColumnFamilyNew);
            await createProjectionStorageTaskNew.ConfigureAwait(false);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[Projection Store] Initialized projection store with column family `{projectionColumnFamilyNew}`", projectionColumnFamilyNew);

            return createProjectionStorageTaskNew.IsCompletedSuccessfully;
        }
        catch (Exception ex) when (True(() => logger.LogError(ex, "Failed to initialize projection version {version}", version)))
        {
            return false;
        }
    }

    // Use this in v12
    //public async Task<bool> InitializeAsync(ProjectionVersion version)
    //{
    //    try
    //    {
    //        await partitionsSchema.CreateProjectionPartitionsStorage(); // partitions

    //        string projectionColumnFamilyNew = naming.GetColumnFamilyNew(version);
    //        if (logger.IsEnabled(LogLevel.Debug))
    //            logger.LogDebug("[Projection Store] Initializing projection store with column family `{projectionColumnFamilyNew}`...", projectionColumnFamilyNew); // new store
    //        Task createProjectionStorageTaskNew = projectionsSchemaNew.CreateProjectionStorageNewAsync(projectionColumnFamilyNew);
    //        await createProjectionStorageTaskNew.ConfigureAwait(false);

    //        if (logger.IsEnabled(LogLevel.Debug))
    //            logger.LogDebug("[Projection Store] Initialized projection store with column family `{projectionColumnFamilyNew}`", projectionColumnFamilyNew);

    //        return createProjectionStorageTaskNew.IsCompletedSuccessfully;
    //    }
    //    catch (Exception ex) when (True(() => logger.LogError(ex, "Failed to initialize projection version {version}", version)))
    //    {
    //        return false;
    //    }
    //}
}

