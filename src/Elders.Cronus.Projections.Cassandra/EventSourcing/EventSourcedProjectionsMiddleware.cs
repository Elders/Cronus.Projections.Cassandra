using Elders.Cronus.DomainModeling;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Middleware;
using System.Collections.Generic;
using System.Linq;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class EventSourcedProjectionsMiddleware : Middleware<HandlerContext>
    {
        static int numberofEventsWhenSnapshot = 5;

        readonly IProjectionStore projectionStore;
        readonly ISnapshotStore snapshotStore;

        public EventSourcedProjectionsMiddleware(IProjectionStore projectionStore, ISnapshotStore snapshotStore)
        {
            this.projectionStore = projectionStore;
            this.snapshotStore = snapshotStore;
        }

        protected override void Run(Execution<HandlerContext> execution)
        {
            var cronusMessage = execution.Context.CronusMessage;
            var projectionDefinition = execution.Context.HandlerInstance as IProjectionDefinition;
            if (projectionDefinition != null)
            {
                if (execution.Context.Message is IEvent)
                {
                    // infrastructure work
                    var projectionId = projectionDefinition.GetProjectionId(execution.Context.Message as IEvent);
                    var snapshot = snapshotStore.Load(projectionId);
                    projectionDefinition.InitializeState(snapshot.State);
                    var projectionCommits = projectionStore.Load(projectionId, snapshot.Revision);

                    var revisionInfo = GetNextRevisionForProjectionCommit(projectionCommits);
                    var commit = new ProjectionCommit(projectionId, revisionInfo.NextCommitRevision, execution.Context.Message as IEvent, cronusMessage.GetEventOrigin());
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

                        if (snapshotGroup.Key == revisionInfo.NextSnapshotRevision && snapshot.Revision < revisionInfo.NextSnapshotRevision)
                            snapshotStore.Save(projectionId, new Snapshot(projectionDefinition.State, revisionInfo.NextSnapshotRevision));
                    }

                    projectionDefinition.Apply(commit.Event);
                }
            }
        }

        RevisionInfo GetNextRevisionForProjectionCommit(IEnumerable<ProjectionCommit> commitsSinceLastSnapshot)
        {
            var highestMarker = commitsSinceLastSnapshot.Max(x => x.SnapshotMarker);
            var numberOfCommitsWithHighestMarker = commitsSinceLastSnapshot.Count(x => x.SnapshotMarker == highestMarker);
            var nextRevision = numberOfCommitsWithHighestMarker > numberofEventsWhenSnapshot
                ? highestMarker + 1
                : highestMarker;

            return new RevisionInfo()
            {
                NextCommitRevision = nextRevision,
                NextSnapshotRevision = nextRevision - 1
            };
        }

        class RevisionInfo
        {
            public int NextCommitRevision { get; set; }
            public int NextSnapshotRevision { get; set; }
            public bool ShouldSnapshot { get; set; }
        }
    }
}
