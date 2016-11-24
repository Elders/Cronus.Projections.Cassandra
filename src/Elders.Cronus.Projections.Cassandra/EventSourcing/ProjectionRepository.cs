using Elders.Cronus.DomainModeling;
using System.Linq;

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

        public ProjectionGetResult<T> Get<T>(IBlobId projectionId) where T : IProjectionDefinition
        {
            var snapshot = snapshotStore.Load(projectionId);
            var projectionCommits = projectionStore.Load(projectionId, snapshot.Revision).ToList();
            var projectionStream = new ProjectionStream(projectionCommits, snapshot);
            return projectionStream.RestoreFromHistory<T>();
        }
    }
}
