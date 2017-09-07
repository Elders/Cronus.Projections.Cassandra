using Elders.Cronus.DomainModeling;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Middleware;
using System;
using Elders.Cronus.Projections.Cassandra.Snapshots;
using Elders.Cronus.Projections.Cassandra.Config;
using Elders.Cronus.DomainModeling.Projections;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class EventSourcedProjectionsMiddleware : Middleware<HandleContext>
    {
        readonly IProjectionStore projectionStore;
        readonly ISnapshotStore snapshotStore;
        readonly ISnapshotStrategy snapshotStrategy;

        public EventSourcedProjectionsMiddleware(IProjectionStore projectionStore, ISnapshotStore snapshotStore, ISnapshotStrategy snapshotStrategy)
        {
            if (ReferenceEquals(null, projectionStore) == true) throw new ArgumentNullException(nameof(projectionStore));
            if (ReferenceEquals(null, snapshotStore) == true) throw new ArgumentNullException(nameof(snapshotStore));
            if (ReferenceEquals(null, snapshotStrategy) == true) throw new ArgumentNullException(nameof(snapshotStrategy));

            this.projectionStore = projectionStore;
            this.snapshotStore = snapshotStore;
            this.snapshotStrategy = snapshotStrategy;
        }

        protected override void Run(Execution<HandleContext> execution)
        {
            CronusMessage cronusMessage = execution.Context.Message;

            Type projectionType = execution.Context.HandlerType;
            var projection = FastActivator.CreateInstance(projectionType) as IProjectionDefinition;

            if (projection != null)
            {
                if (cronusMessage.Payload is IEvent)
                {
                    var projectionIds = projection.GetProjectionIds(cronusMessage.Payload as IEvent);
                    string contractId = projectionType.GetContractId();

                    foreach (var projectionId in projectionIds)
                    {
                        // We should be using IProjectionRepository here!!
                        ISnapshot snapshot = snapshotStore.Load(contractId, projectionId);
                        ProjectionStream projectionStream = projectionStore.Load(contractId, projectionId, snapshot);
                        if (ReferenceEquals(null, projectionStream) == true) throw new ArgumentException(nameof(projectionStream));


                        var projectionCommits = projectionStream.Commits;
                        int snapshotMarker = snapshotStrategy.GetSnapshotMarker(projectionCommits, snapshot.Revision);
                        var commit = new ProjectionCommit(projectionId, contractId, cronusMessage.Payload as IEvent, snapshotMarker, cronusMessage.GetEventOrigin(), DateTime.UtcNow);
                        projectionStore.Save(commit);

                        var we = snapshotStrategy.ShouldCreateSnapshot(projectionCommits, snapshot.Revision);
                        if (we.ShouldCreateSnapshot)
                        {
                            var queryResult = projectionStream.RestoreFromHistory(projectionType);
                            snapshotStore.Save(new Snapshot(projectionId, contractId, queryResult.Projection.State, we.KeepTheNextSnapshotRevisionHere));
                        }
                    }
                }
            }
        }
    }
}
