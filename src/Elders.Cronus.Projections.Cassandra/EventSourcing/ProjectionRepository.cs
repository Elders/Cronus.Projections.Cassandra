using Elders.Cronus.DomainModeling;
using Elders.Cronus.Projections.Cassandra.Snapshots;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class ProjectionRepository : IProjectionRepository
    {
        readonly IProjectionStore projectionStore;
        readonly ISnapshotStore snapshotStore;

        public ProjectionRepository(IProjectionStore projectionStore, ISnapshotStore snapshotStore)
        {
            this.projectionStore = projectionStore;
            this.snapshotStore = snapshotStore;
        }

        public IProjectionGetResult<T> Get<T>(IBlobId projectionId) where T : IProjectionDefinition
        {
            var snapshot = snapshotStore.Load(typeof(T), projectionId);
            var projectionStream = projectionStore.Load<T>(projectionId, snapshot);
            return projectionStream.RestoreFromHistory<T>();
        }
    }
}
