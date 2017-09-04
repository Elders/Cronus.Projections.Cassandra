using Elders.Cronus.DomainModeling;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Middleware;
using System.Linq;
using System;
using Elders.Cronus.Projections.Cassandra.Snapshots;
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
            var cronusMessage = execution.Context.Message;

            Type projectionType = execution.Context.HandlerType;
            var projectionDefinition = FastActivator.CreateInstance(projectionType) as IProjectionDefinition;
            if (projectionDefinition != null)
            {
                if (execution.Context.Message.Payload is IEvent)
                {
                    // Check if this is replay
                    string isReplayHeader;
                    var isReplay = false;
                    if (execution.Context.Message.Headers.TryGetValue("isReplay", out isReplayHeader))
                        bool.TryParse(isReplayHeader, out isReplay);

                    var projectionIds = projectionDefinition.GetProjectionIds(execution.Context.Message.Payload as IEvent);
                    string contractId = projectionType.GetContractId();
                    foreach (var projectionId in projectionIds)
                    {
                        // infrastructure work
                        var snapshot = snapshotStore.Load(contractId, projectionId, isReplay);
                        projectionDefinition.InitializeState(projectionId, snapshot.State);

                        var projectionCommits = projectionStore.Load(contractId, projectionId, snapshot, isReplay).Commits;
                        var snapshotMarker = snapshotStrategy.GetSnapshotMarker(projectionCommits);
                        var commit = new ProjectionCommit(projectionId, contractId, execution.Context.Message.Payload as IEvent, snapshotMarker, cronusMessage.GetEventOrigin(), DateTime.UtcNow);
                        projectionStore.Save(commit, isReplay);

                        //  Realproj work
                        var groupedBySnapshotMarker = projectionCommits.GroupBy(x => x.SnapshotMarker).OrderBy(x => x.Key);
                        foreach (var snapshotGroup in groupedBySnapshotMarker)
                        {
                            var eventsByAggregate = snapshotGroup.GroupBy(x => x.EventOrigin.AggregateRootId);
                            foreach (var aggregateGroup in eventsByAggregate)
                            {
                                var events = aggregateGroup
                                    .OrderBy(x => x.EventOrigin.AggregateRevision)
                                    .ThenBy(x => x.EventOrigin.AggregateEventPosition)
                                    .Select(x => x.Event);
                                projectionDefinition.ReplayEvents(events);
                            }

                            if (snapshotGroup.Key > snapshot.Revision && snapshotStrategy.ShouldCreateSnapshot(snapshotGroup))
                                snapshotStore.Save(new Snapshot(projectionId, contractId, projectionDefinition.State, snapshotGroup.Key), isReplay);
                        }

                        projectionDefinition.Apply(commit.Event);
                    }
                }
            }
        }
    }
}
