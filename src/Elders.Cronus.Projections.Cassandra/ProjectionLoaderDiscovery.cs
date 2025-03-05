using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.Discoveries;
using Elders.Cronus.EventStore;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Elders.Cronus.Projections.Versioning;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Projections.Cassandra;

internal static class V11Only
{
    /// <summary>
    /// For v12 we will need an initializer which will be creating all tables with `new` and the other tables have to be manually removed from the database (or some custom migration to take care about this task).
    /// </summary>
    /// <param name="services"></param>
    /// <returns></returns>
    internal static IServiceCollection AddProjectionStoreInitializers_v11(this IServiceCollection services)
    {
        services.AddTransient<IProjectionVersionFinder, LatestVersionProjectionFinder>();
        services.AddTransient<CassandraProjectionStoreInitializer>();
        services.AddTransient<CassandraProjectionStoreInitializerNew>();
        services.AddTenantSingleton<IInitializableProjectionStore, CassandraProjectionStoreInitializerNew>();

        return services;
    }
}

public class ProjectionLoaderDiscovery : DiscoveryBase<IProjectionReader>
{
    protected override DiscoveryResult<IProjectionReader> DiscoverFromAssemblies(DiscoveryContext context)
    {
        return new DiscoveryResult<IProjectionReader>(GetModels(context), services => services
                                                                                            .AddProjectionStoreInitializers_v11()
                                                                                            .AddOptions<CassandraProviderOptions, CassandraProviderOptionsProvider>()
                                                                                            .AddOptions<TableRetentionOptions, TableRetentionOptionsProvider>());
    }

    IEnumerable<DiscoveredModel> GetModels(DiscoveryContext context)
    {
        // settings
        var cassandraSettings = context.FindService<ICassandraProjectionStoreSettings>();
        foreach (Type setting in cassandraSettings)
        {
            yield return new DiscoveredModel(setting, setting, ServiceLifetime.Transient);
        }

        // schema
        // Right now the 3 lines bellow are replaced with AddProjectionStoreInitializers_v11 but only for v11 version. For the next we need to think a bit more what to do and inject.
        // Note the following line there services.AddTransient<IProjectionVersionFinder, LatestVersionProjectionFinder>();
        //yield return new DiscoveredModel(typeof(CassandraProjectionStoreInitializer), typeof(CassandraProjectionStoreInitializer), ServiceLifetime.Transient) { CanOverrideDefaults = true };
        //yield return new DiscoveredModel(typeof(KaliProjectionStoreInitializer), typeof(KaliProjectionStoreInitializer), ServiceLifetime.Transient) { CanOverrideDefaults = true };
        //yield return new DiscoveredModel(typeof(IInitializableProjectionStore), provider => provider.GetRequiredService<SingletonPerTenant<KaliProjectionStoreInitializer>>().Get(), ServiceLifetime.Transient); // 

        yield return new DiscoveredModel(typeof(CassandraProjectionStoreSchema), typeof(CassandraProjectionStoreSchema), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(IProjectionStoreStorageManager), typeof(CassandraProjectionStoreSchema), ServiceLifetime.Singleton);

        yield return new DiscoveredModel(typeof(CassandraProjectionPartitionStoreSchema), typeof(CassandraProjectionPartitionStoreSchema), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(ICassandraProjectionPartitionStoreSchema), typeof(CassandraProjectionPartitionStoreSchema), ServiceLifetime.Singleton);

        yield return new DiscoveredModel(typeof(CassandraProjectionStoreSchemaNew), typeof(CassandraProjectionStoreSchemaNew), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(ICassandraProjectionStoreSchemaNew), typeof(CassandraProjectionStoreSchemaNew), ServiceLifetime.Singleton);

        // cassandra
        yield return new DiscoveredModel(typeof(CassandraProvider), typeof(CassandraProvider), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(ICassandraProvider), typeof(CassandraProvider), ServiceLifetime.Singleton);

        // projections
        yield return new DiscoveredModel(typeof(IProjectionStore), typeof(CassandraMigrationStore), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(CassandraMigrationStore), typeof(CassandraMigrationStore), ServiceLifetime.Singleton);

        // old store
        yield return new DiscoveredModel(typeof(IProjectionStoreLegacy), typeof(CassandraProjectionStore), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(CassandraProjectionStore), typeof(CassandraProjectionStore), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(CassandraProjectionStore<>), typeof(CassandraProjectionStore<>), ServiceLifetime.Singleton);

        // new store
        yield return new DiscoveredModel(typeof(IProjectionStoreNew), typeof(CassandraProjectionStoreNew), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(CassandraProjectionStoreNew), typeof(CassandraProjectionStoreNew), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(CassandraProjectionStoreNew<>), typeof(CassandraProjectionStoreNew<>), ServiceLifetime.Singleton);

        // partitions store
        yield return new DiscoveredModel(typeof(IProjectionPartionsStore), typeof(CassandraProjectionPartitionsStore), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(CassandraProjectionPartitionsStore), typeof(CassandraProjectionPartitionsStore), ServiceLifetime.Singleton);

        // naming
        yield return new DiscoveredModel(typeof(IKeyspaceNamingStrategy), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(NoKeyspaceNamingStrategy), typeof(NoKeyspaceNamingStrategy), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(KeyspacePerTenantKeyspace), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Singleton);

        var projectionTypes = context.FindService<IProjectionDefinition>().ToList();
        yield return new DiscoveredModel(typeof(ProjectionsProvider), provider => new ProjectionsProvider(projectionTypes), ServiceLifetime.Singleton);

        yield return new DiscoveredModel(typeof(CassandraReplicationStrategyFactory), typeof(CassandraReplicationStrategyFactory), ServiceLifetime.Singleton);
        yield return new DiscoveredModel(typeof(ICassandraReplicationStrategy), provider => provider.GetRequiredService<CassandraReplicationStrategyFactory>().GetReplicationStrategy(), ServiceLifetime.Transient);

        yield return new DiscoveredModel(typeof(VersionedProjectionsNaming), typeof(VersionedProjectionsNaming), ServiceLifetime.Singleton);


        //yield return new DiscoveredModel(typeof(InMemoryProjectionVersionStore), typeof(InMemoryProjectionVersionStore), ServiceLifetime.Singleton);

        //yield return new DiscoveredModel(typeof(IProjectionTableRetentionStrategy), typeof(RetainOldProjectionRevisions), ServiceLifetime.Transient);
        //yield return new DiscoveredModel(typeof(RetainOldProjectionRevisions), typeof(RetainOldProjectionRevisions), ServiceLifetime.Transient);


    }
}

class CassandraReplicationStrategyFactory
{
    private readonly CassandraProviderOptions options;

    public CassandraReplicationStrategyFactory(IOptionsMonitor<CassandraProviderOptions> optionsMonitor)
    {
        this.options = optionsMonitor.CurrentValue;
    }

    internal ICassandraReplicationStrategy GetReplicationStrategy()
    {
        ICassandraReplicationStrategy replicationStrategy = null;
        if (options.ReplicationStrategy.Equals("simple", StringComparison.OrdinalIgnoreCase))
        {
            replicationStrategy = new SimpleReplicationStrategy(options.ReplicationFactor);
        }
        else if (options.ReplicationStrategy.Equals("network_topology", StringComparison.OrdinalIgnoreCase))
        {
            var settings = new List<NetworkTopologyReplicationStrategy.DataCenterSettings>();
            foreach (var datacenter in options.Datacenters)
            {
                var setting = new NetworkTopologyReplicationStrategy.DataCenterSettings(datacenter, options.ReplicationFactor);
                settings.Add(setting);
            }
            replicationStrategy = new NetworkTopologyReplicationStrategy(settings);
        }

        return replicationStrategy;
    }
}

/// <summary>
/// Use this class ONLY for version v11 for cronus. Remove this later when we dont use the legacy tables. USE <see cref="ProjectionFinderViaReflection"/> instead
/// We put this because we need to ensure that we have the _new table with the latest live revision (we always append in both projection tables)
/// Otherwise if we have latest live version 6 , until we go manually to rebuild the projection the only _new table for this projection will be the initial `1`
/// </summary>
internal class LatestVersionProjectionFinder : IProjectionVersionFinder
{
    private const char Dash = '-';

    private readonly TypeContainer<IProjection> _allProjections;
    private readonly ProjectionHasher _hasher;
    private readonly IProjectionStore _projectionStore;
    private readonly ICronusContextAccessor _cronusContextAccessor;
    private readonly ProjectionVersion _f1Live;
    private readonly ILogger<LatestVersionProjectionFinder> _logger;

    public LatestVersionProjectionFinder(TypeContainer<IProjection> allProjections, ProjectionHasher hasher, IProjectionStore projectionStore, ICronusContextAccessor cronusContextAccessor, ILogger<LatestVersionProjectionFinder> logger)
    {
        _allProjections = allProjections;
        _hasher = hasher;
        _projectionStore = projectionStore;
        _cronusContextAccessor = cronusContextAccessor;
        _f1Live = new ProjectionVersion(ProjectionVersionsHandler.ContractId, ProjectionStatus.Live, 1, hasher.CalculateHash(typeof(ProjectionVersionsHandler)));

        _logger = logger;
    }

    public IEnumerable<ProjectionVersion> GetProjectionVersionsToBootstrap()
    {
        foreach (Type projectionType in _allProjections.Items)
        {
            if (typeof(IProjectionDefinition).IsAssignableFrom(projectionType) || typeof(IAmEventSourcedProjection).IsAssignableFrom(projectionType))
            {
                var loadedVersion = GetCurrentLiveVersionOrTheDefaultOne(projectionType).GetAwaiter().GetResult();
                if (loadedVersion is not null)
                    yield return loadedVersion;
            }
        }
    }

    public async Task<ProjectionVersion> GetCurrentLiveVersionOrTheDefaultOne(Type projectionType)
    {
        string projectionName = projectionType.GetContractId();

        try
        {
            var loadResultFromF1 = await GetProjectionVersionsFromStoreAsync(projectionName, _cronusContextAccessor.CronusContext.Tenant).ConfigureAwait(false);

            if (loadResultFromF1.IsSuccess)
            {
                ProjectionVersion found = loadResultFromF1.Data.State.AllVersions.GetLive();
                if (found is not null)
                    return found;
            }

            return null;
        }
        catch (InvalidQueryException ex)
        {
            return null;
        }
        catch (Exception ex) when (True(() => _logger.LogError(ex, "Something went wrong while getting the latest live version, because {message}", ex.Message)))
        {
            throw;
        }
    }

    private async Task<ReadResult<ProjectionVersionsHandler>> GetProjectionVersionsFromStoreAsync(string projectionName, string tenant)
    {
        ProjectionVersionManagerId versionId = new ProjectionVersionManagerId(projectionName, tenant);
        ProjectionStream stream = await LoadProjectionStreamAsync(versionId, _f1Live).ConfigureAwait(false);

        ProjectionVersionsHandler projectionInstance = new ProjectionVersionsHandler();
        projectionInstance = await stream.RestoreFromHistoryAsync(projectionInstance).ConfigureAwait(false);

        return new ReadResult<ProjectionVersionsHandler>(projectionInstance);
    }

    private async Task<ProjectionStream> LoadProjectionStreamAsync(IBlobId projectionId, ProjectionVersion version)
    {
        List<ProjectionCommit> projectionCommits = new List<ProjectionCommit>();

        ProjectionStream stream = ProjectionStream.Empty();

        ProjectionQueryOptions options = new ProjectionQueryOptions(projectionId, version, new PagingOptions(1000, null, Order.Ascending));
        ProjectionsOperator @operator = new ProjectionsOperator()
        {
            OnProjectionStreamLoadedAsync = projectionStream =>
            {
                stream = projectionStream;
                return Task.CompletedTask;
            }
        };

        await _projectionStore.EnumerateProjectionsAsync(@operator, options).ConfigureAwait(false);

        return stream;
    }
}
