using Elders.Cronus.DomainModeling;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Middleware;
using System.Linq;
using System;
using Elders.Cronus.Projections.Cassandra.Snapshots;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class EventSourcedProjectionsMiddleware : Middleware<HandleContext>
    {
        readonly IProjectionStore projectionStore;
        readonly ISnapshotStore snapshotStore;
        readonly ISnapshotStrategy snapshotStrategy;

        public EventSourcedProjectionsMiddleware(IProjectionStore projectionStore, ISnapshotStore snapshotStore, ISnapshotStrategy snapshotStrategy)
        {
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
                    // infrastructure work
                    var projectionId = projectionDefinition.GetProjectionId(execution.Context.Message.Payload as IEvent);
                    var snapshot = snapshotStore.Load(projectionType, projectionId);
                    projectionDefinition.InitializeState(snapshot.State);
                    var projectionCommits = projectionStore.Load(projectionType, projectionId, snapshot).Commits;


                    var snapshotMarker = snapshotStrategy.GetSnapshotMarker(projectionCommits);
                    var commit = new ProjectionCommit(projectionId, projectionType, execution.Context.Message.Payload as IEvent, snapshotMarker, cronusMessage.GetEventOrigin(), DateTime.UtcNow);
                    projectionStore.Save(commit);

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
                            snapshotStore.Save(new Snapshot(projectionId, projectionType, projectionDefinition.State, snapshotGroup.Key));
                    }

                    projectionDefinition.Apply(commit.Event);
                }
            }
        }
    }
}
