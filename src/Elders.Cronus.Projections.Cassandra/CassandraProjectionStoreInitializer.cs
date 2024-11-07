using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra;

public class CassandraProjectionStoreInitializer : IInitializableProjectionStore
{
    static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraProjectionStoreInitializer));

    private readonly IProjectionStoreStorageManager projectionsSchema;
    private readonly VersionedProjectionsNaming naming;

    public CassandraProjectionStoreInitializer(IProjectionStoreStorageManager projectionsSchema, VersionedProjectionsNaming naming)
    {
        if (projectionsSchema is null) throw new ArgumentNullException(nameof(projectionsSchema));

        this.naming = naming;
        this.projectionsSchema = projectionsSchema;
    }

    public async Task<bool> InitializeAsync(ProjectionVersion version)
    {
        try
        {
            string projectionColumnFamily = naming.GetColumnFamily(version);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[Projection Store] Initializing projection store with column family `{projectionColumnFamily}`...", projectionColumnFamily);
            Task createProjectionStorageTask = projectionsSchema.CreateProjectionsStorageAsync(projectionColumnFamily);
            await createProjectionStorageTask.ConfigureAwait(false);
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug( "[Projection Store] Initialized projection store with column family `{projectionColumnFamily}`", projectionColumnFamily);

            return createProjectionStorageTask.IsCompletedSuccessfully;
        }
        catch (Exception ex) when (True(() => logger.LogError(ex, "Failed to initialize projection version {version}", version)))
        {
            return false;
        }
    }
}
