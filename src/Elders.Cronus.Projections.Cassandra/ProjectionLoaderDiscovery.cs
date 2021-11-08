using System;
using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.Discoveries;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Elders.Cronus.Projections.Snapshotting;
using Elders.Cronus.Projections.Versioning;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Projections.Cassandra
{
    public class ProjectionLoaderDiscovery : DiscoveryBase<IProjectionReader>
    {
        protected override DiscoveryResult<IProjectionReader> DiscoverFromAssemblies(DiscoveryContext context)
        {
            return new DiscoveryResult<IProjectionReader>(GetModels(context), services => services.AddOptions<CassandraProviderOptions, CassandraProviderOptionsProvider>()
                                                                                                  .AddOptions<TableRetentionOptions, TableRetentionOptionsProvider>());
        }

        IEnumerable<DiscoveredModel> GetModels(DiscoveryContext context)
        {
            // settings
            var cassandraSettings = context.FindService<ICassandraProjectionStoreSettings>();
            foreach (var setting in cassandraSettings)
            {
                yield return new DiscoveredModel(setting, setting, ServiceLifetime.Transient);
            }

            // main interfaces
            //yield return new DiscoveredModel(typeof(IProjectionReader), typeof(ProjectionRepository), ServiceLifetime.Transient);
            //yield return new DiscoveredModel(typeof(IProjectionWriter), typeof(ProjectionRepository), ServiceLifetime.Transient);
            //yield return new DiscoveredModel(typeof(ProjectionRepository), typeof(ProjectionRepository), ServiceLifetime.Transient);
            //yield return new DiscoveredModel(typeof(ProjectionRepositoryWithFallback<>), typeof(ProjectionRepositoryWithFallback<>), ServiceLifetime.Transient);
            //yield return new DiscoveredModel(typeof(ProjectionRepositoryWithFallback<,>), typeof(ProjectionRepositoryWithFallback<,>), ServiceLifetime.Transient);

            // schema
            yield return new DiscoveredModel(typeof(IInitializableProjectionStore), typeof(CassandraProjectionStoreInitializer), ServiceLifetime.Transient)
            {
                CanOverrideDefaults = true
            };
            yield return new DiscoveredModel(typeof(IProjectionStoreStorageManager), typeof(CassandraProjectionStoreSchema), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(CassandraProjectionStoreSchema), typeof(CassandraProjectionStoreSchema), ServiceLifetime.Transient);

            // projection store
            yield return new DiscoveredModel(typeof(IProjectionStore), provider => provider.GetRequiredService<SingletonPerTenant<CassandraProjectionStore>>().Get(), ServiceLifetime.Transient)
            {
                CanOverrideDefaults = true
            };

            yield return new DiscoveredModel(typeof(CassandraProjectionStore), typeof(CassandraProjectionStore), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(CassandraProjectionStore<>), typeof(CassandraProjectionStore<>), ServiceLifetime.Transient);

            // snapshot store
            yield return new DiscoveredModel(typeof(ISnapshotStore), typeof(CassandraSnapshotStore), ServiceLifetime.Transient)
            {
                CanOverrideDefaults = true
            };
            yield return new DiscoveredModel(typeof(CassandraSnapshotStore), typeof(CassandraSnapshotStore), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(CassandraSnapshotStore<>), typeof(CassandraSnapshotStore<>), ServiceLifetime.Transient);

            // cassandra
            yield return new DiscoveredModel(typeof(CassandraProvider), typeof(CassandraProvider), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(ICassandraProvider), provider => provider.GetRequiredService<SingletonPerTenant<CassandraProvider>>().Get(), ServiceLifetime.Transient);

            // naming
            yield return new DiscoveredModel(typeof(IKeyspaceNamingStrategy), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(NoKeyspaceNamingStrategy), typeof(NoKeyspaceNamingStrategy), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(KeyspacePerTenantKeyspace), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Transient);

            var projectionTypes = context.FindService<IProjectionDefinition>().ToList();
            yield return new DiscoveredModel(typeof(ProjectionsProvider), provider => new ProjectionsProvider(projectionTypes), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(CassandraSnapshotStoreSchema), typeof(CassandraSnapshotStoreSchema), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(CassandraReplicationStrategyFactory), typeof(CassandraReplicationStrategyFactory), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(ICassandraReplicationStrategy), provider => provider.GetRequiredService<CassandraReplicationStrategyFactory>().GetReplicationStrategy(), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(VersionedProjectionsNaming), typeof(VersionedProjectionsNaming), ServiceLifetime.Singleton);

            yield return new DiscoveredModel(typeof(ISnapshotStrategy), provider => new EventsCountSnapshotStrategy(100), ServiceLifetime.Singleton)
            {
                CanOverrideDefaults = true
            };

            yield return new DiscoveredModel(typeof(InMemoryProjectionVersionStore), typeof(InMemoryProjectionVersionStore), ServiceLifetime.Singleton);

            yield return new DiscoveredModel(typeof(IProjectionTableRetentionStrategy), typeof(RetainOldProjectionRevisions), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(RetainOldProjectionRevisions), typeof(RetainOldProjectionRevisions), ServiceLifetime.Transient);


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
}
