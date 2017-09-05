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
        readonly ISnapshotStrategy snapshotStrategy;

        public ProjectionRepository(IProjectionStore projectionStore, ISnapshotStore snapshotStore, ISnapshotStrategy snapshotStrategy)
        {
            if (ReferenceEquals(null, projectionStore) == true) throw new ArgumentException(nameof(projectionStore));
            if (ReferenceEquals(null, snapshotStore) == true) throw new ArgumentException(nameof(snapshotStore));
            if (ReferenceEquals(null, snapshotStrategy) == true) throw new ArgumentException(nameof(snapshotStrategy));

            this.projectionStore = projectionStore;
            this.snapshotStore = snapshotStore;
            this.snapshotStrategy = snapshotStrategy;
        }

        public IProjectionGetResult<T> Get<T>(IBlobId projectionId) where T : IProjectionDefinition
        {
            string contractId = typeof(T).GetContractId();

            ISnapshot snapshot = snapshotStore.Load(contractId, projectionId);
            ProjectionStream projectionStream = projectionStore.Load(contractId, projectionId, snapshot);
            if (ReferenceEquals(null, projectionStream) == true) throw new ArgumentException(nameof(projectionStream));
            var queryResult = projectionStream.RestoreFromHistory<T>();

            var shouldCreateSnapshot = snapshotStrategy.ShouldCreateSnapshot(projectionStream.Commits, snapshot.Revision);
            if (shouldCreateSnapshot.ShouldCreateSnapshot)
                snapshotStore.Save(new Snapshot(projectionId, contractId, queryResult.Projection.State, shouldCreateSnapshot.KeepTheNextSnapshotRevisionHere));

            return queryResult;
        }
    }
}
