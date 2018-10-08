using System.Linq;
using Elders.Cronus.Discoveries;
using Elders.Cronus.Projections.Cassandra.Config;
using Elders.Cronus.Projections.Cassandra.EventSourcing;
using Elders.Cronus.Projections.Cassandra.Snapshots;
using Elders.Cronus.Projections.Snapshotting;
using Elders.Cronus.Projections.Versioning;

namespace Elders.Cronus.Projections.Cassandra
{
    public class ProjectionLoaderDiscovery : DiscoveryBasedOnExecutingDirAssemblies<IProjectionLoader>
    {
        protected override DiscoveryResult<IProjectionLoader> DiscoverFromAssemblies(DiscoveryContext context)
        {
            var projectionTypes = context.Assemblies.SelectMany(ass => ass.GetTypes().Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x)));

            DiscoveryResult<IProjectionLoader> result = new DiscoveryResult<IProjectionLoader>();

            result.Models.Add(new DiscoveredModel(typeof(CassandraProvider), typeof(CassandraProvider)));
            result.Models.Add(new DiscoveredModel(typeof(CassandraProjectionStoreSchema), typeof(CassandraProjectionStoreSchema)));
            result.Models.Add(new DiscoveredModel(typeof(IProjectionStore), typeof(CassandraProjectionStore)));

            result.Models.Add(new DiscoveredModel(typeof(ProjectionsProvider), typeof(ProjectionsProvider), new ProjectionsProvider(projectionTypes)));
            result.Models.Add(new DiscoveredModel(typeof(CassandraSnapshotStoreSchema), typeof(CassandraSnapshotStoreSchema)));
            result.Models.Add(new DiscoveredModel(typeof(ISnapshotStore), typeof(CassandraSnapshotStore)));

            result.Models.Add(new DiscoveredModel(typeof(ISnapshotStrategy), typeof(EventsCountSnapshotStrategy), new EventsCountSnapshotStrategy(100)));

            result.Models.Add(new DiscoveredModel(typeof(InMemoryProjectionVersionStore), typeof(InMemoryProjectionVersionStore)));

            result.Models.Add(new DiscoveredModel(typeof(IProjectionLoader), typeof(ProjectionRepository)));

            return result;
        }
    }
}
