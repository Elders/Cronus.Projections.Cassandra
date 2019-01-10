using System;
using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.Discoveries;
using Elders.Cronus.Projections.Cassandra.ReplicationStrategies;
using Elders.Cronus.Projections.Snapshotting;
using Elders.Cronus.Projections.Versioning;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Projections.Cassandra
{
    public class ProjectionLoaderDiscovery : DiscoveryBase<IProjectionReader>
    {
        protected override DiscoveryResult<IProjectionReader> DiscoverFromAssemblies(DiscoveryContext context)
        {
            return new DiscoveryResult<IProjectionReader>(GetModels(context));
        }

        IEnumerable<DiscoveredModel> GetModels(DiscoveryContext context)
        {
            // options
            yield return new DiscoveredModel(typeof(IConfigureOptions<CassandraProviderOptions>), typeof(CassandraProviderOptionsProvider), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(IOptionsChangeTokenSource<CassandraProviderOptions>), typeof(CassandraProviderOptionsProvider), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(IOptionsFactory<CassandraProviderOptions>), typeof(CassandraProviderOptionsProvider), ServiceLifetime.Singleton);

            // settings
            var cassandraSettings = context.Assemblies.SelectMany(asm => asm.GetLoadableTypes())
                .Where(type => type.IsAbstract == false && type.IsInterface == false && typeof(ICassandraProjectionStoreSettings).IsAssignableFrom(type));
            foreach (var setting in cassandraSettings)
            {
                yield return new DiscoveredModel(setting, setting, ServiceLifetime.Transient);
            }

            // main interfaces
            yield return new DiscoveredModel(typeof(IProjectionReader), typeof(ProjectionRepository), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(IProjectionWriter), typeof(ProjectionRepository), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(ProjectionRepository), typeof(ProjectionRepository), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(ProjectionRepositoryWithFallback<>), typeof(ProjectionRepositoryWithFallback<>), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(ProjectionRepositoryWithFallback<,>), typeof(ProjectionRepositoryWithFallback<,>), ServiceLifetime.Transient);

            // schema
            yield return new DiscoveredModel(typeof(IInitializableProjectionStore), typeof(CassandraProjectionStoreInitializer), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(IProjectionStoreStorageManager), typeof(CassandraProjectionStoreSchema), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(CassandraProjectionStoreSchema), typeof(CassandraProjectionStoreSchema), ServiceLifetime.Transient);

            // projection store
            yield return new DiscoveredModel(typeof(IProjectionStore), typeof(CassandraProjectionStore), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(CassandraProjectionStore), typeof(CassandraProjectionStore), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(CassandraProjectionStore<>), typeof(CassandraProjectionStore<>), ServiceLifetime.Transient);

            // snapshot store
            yield return new DiscoveredModel(typeof(ISnapshotStore), typeof(CassandraSnapshotStore), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(CassandraSnapshotStore), typeof(CassandraSnapshotStore), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(CassandraSnapshotStore<>), typeof(CassandraSnapshotStore<>), ServiceLifetime.Transient);

            // cassandra
            yield return new DiscoveredModel(typeof(CassandraProvider), typeof(CassandraProvider), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(ICassandraProvider), provider => provider.GetRequiredService<SingletonPerTenant<CassandraProvider>>().Get(), ServiceLifetime.Transient);

            // naming
            yield return new DiscoveredModel(typeof(IKeyspaceNamingStrategy), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(NoKeyspaceNamingStrategy), typeof(NoKeyspaceNamingStrategy), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(KeyspacePerTenantKeyspace), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Transient);

            var projectionTypes = context.Assemblies.SelectMany(ass => ass.GetLoadableTypes().Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x))).ToList();
            yield return new DiscoveredModel(typeof(ProjectionsProvider), provider => new ProjectionsProvider(projectionTypes), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(CassandraSnapshotStoreSchema), typeof(CassandraSnapshotStoreSchema), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(ICassandraReplicationStrategy), provider => GetReplicationStrategy(context.Configuration), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IProjectionsNamingStrategy), typeof(VersionedProjectionsNaming), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(VersionedProjectionsNaming), typeof(VersionedProjectionsNaming), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(ISnapshotStrategy), provider => new EventsCountSnapshotStrategy(100), ServiceLifetime.Singleton);

            yield return new DiscoveredModel(typeof(InMemoryProjectionVersionStore), typeof(InMemoryProjectionVersionStore), ServiceLifetime.Singleton);
        }


        int GetReplicationFactor(IConfiguration configuration)
        {
            var replFactorCfg = configuration["cronus_projections_cassandra_replication_factor"];
            return string.IsNullOrEmpty(replFactorCfg) ? 1 : int.Parse(replFactorCfg);
        }

        ICassandraReplicationStrategy GetReplicationStrategy(IConfiguration configuration)
        {
            var replStratefyCfg = configuration["cronus_projections_cassandra_replication_strategy"];
            var replFactorCfg = configuration["cronus_projections_cassandra_replication_factor"];

            ICassandraReplicationStrategy replicationStrategy = null;
            if (string.IsNullOrEmpty(replStratefyCfg))
            {
                replicationStrategy = new SimpleReplicationStrategy(1);
            }
            else if (replStratefyCfg.Equals("simple", StringComparison.OrdinalIgnoreCase))
            {
                replicationStrategy = new SimpleReplicationStrategy(GetReplicationFactor(configuration));
            }
            else if (replStratefyCfg.Equals("network_topology", StringComparison.OrdinalIgnoreCase))
            {
                int replicationFactor = GetReplicationFactor(configuration);
                var settings = new List<NetworkTopologyReplicationStrategy.DataCenterSettings>();
                string[] datacenters = configuration["cronus_projections_cassandra__datacenters"].Split(',');
                foreach (var datacenter in datacenters)
                {
                    var setting = new NetworkTopologyReplicationStrategy.DataCenterSettings(datacenter, replicationFactor);
                    settings.Add(setting);
                }
                replicationStrategy = new NetworkTopologyReplicationStrategy(settings);
            }

            return replicationStrategy;
        }
    }
}
