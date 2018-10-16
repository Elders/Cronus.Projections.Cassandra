using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.Discoveries;
using Elders.Cronus.Projections.Cassandra.Config;
using Elders.Cronus.Projections.Cassandra.EventSourcing;
using Elders.Cronus.Projections.Cassandra.Snapshots;
using Elders.Cronus.Projections.Snapshotting;
using Elders.Cronus.Projections.Versioning;
using Microsoft.Extensions.DependencyInjection;

namespace Elders.Cronus.Projections.Cassandra
{
    public class ProjectionLoaderDiscovery : DiscoveryBasedOnExecutingDirAssemblies<IProjectionReader>
    {
        protected override DiscoveryResult<IProjectionReader> DiscoverFromAssemblies(DiscoveryContext context)
        {
            return new DiscoveryResult<IProjectionReader>(GetModels(context));
        }

        IEnumerable<DiscoveredModel> GetModels(DiscoveryContext context)
        {
            yield return new DiscoveredModel(typeof(CassandraProvider), typeof(CassandraProvider), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(CassandraProjectionStoreSchema), typeof(CassandraProjectionStoreSchema), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(IProjectionStore), typeof(CassandraProjectionStore), ServiceLifetime.Transient);

            var projectionTypes = context.Assemblies.SelectMany(ass => ass.GetLoadableTypes().Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x)));
            yield return new DiscoveredModel(typeof(ProjectionsProvider), provider => new ProjectionsProvider(projectionTypes), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(CassandraSnapshotStoreSchema), typeof(CassandraSnapshotStoreSchema), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(ISnapshotStore), typeof(CassandraSnapshotStore), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(ISnapshotStrategy), provider => new EventsCountSnapshotStrategy(100), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(InMemoryProjectionVersionStore), typeof(InMemoryProjectionVersionStore), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IProjectionReader), typeof(ProjectionRepository), ServiceLifetime.Transient);
        }
    }
}
