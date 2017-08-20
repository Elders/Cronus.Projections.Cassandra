using System;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.DomainModeling.Projections;
using Elders.Cronus.Projections.Cassandra.Snapshots;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class ProjectionRepository : IProjectionRepository
    {
        readonly IProjectionStore projectionStore;
        readonly ISnapshotStore snapshotStore;

        public ProjectionRepository(IProjectionStore projectionStore, ISnapshotStore snapshotStore)
        {
            if (ReferenceEquals(null, projectionStore) == true) throw new ArgumentException(nameof(projectionStore));
            if (ReferenceEquals(null, snapshotStore) == true) throw new ArgumentException(nameof(snapshotStore));

            this.projectionStore = projectionStore;
            this.snapshotStore = snapshotStore;
        }

        public IProjectionGetResult<T> Get<T>(IBlobId projectionId) where T : IProjectionDefinition
        {
            string contractId = typeof(T).GetContractId();
            ISnapshot snapshot = snapshotStore.Load(contractId, projectionId, false);
            ProjectionStream projectionStream = projectionStore.Load(contractId, projectionId, snapshot, false);
            if (ReferenceEquals(null, projectionStream) == true) throw new ArgumentException(nameof(projectionStream));
            return projectionStream.RestoreFromHistory<T>();
        }
    }
}
