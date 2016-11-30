using Elders.Cronus.DomainModeling;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Middleware;
using System.Collections.Generic;
using System.Linq;
using System;
using Elders.Cronus.Pipeline.Config;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{

    public static class EventSourcedProjectionsMiddlewareConfig
    {

        public static Middleware<HandleContext> EventSourcedProjections(this Middleware<HandleContext> self, IProjectionStore projectionStore, ISnapshotStore snapshotStore)
        {
            return new EventSourcedProjectionsMiddleware(projectionStore, snapshotStore);
        }

    }

    public class EventSourcedProjectionsMiddleware : Middleware<HandleContext>
    {
        static int numberofEventsWhenSnapshot = 5;

        readonly IProjectionStore projectionStore;
        readonly ISnapshotStore snapshotStore;

        public EventSourcedProjectionsMiddleware(IProjectionStore projectionStore, ISnapshotStore snapshotStore)
        {
            this.projectionStore = projectionStore;
            this.snapshotStore = snapshotStore;
        }

        protected override void Run(Execution<HandleContext> execution)
        {
            var cronusMessage = execution.Context.Message;
            //nie zname kakuv e tipa nqmame nujda ot factory.
            //to ne trqbva da ima dependancyta
            var projectionDefinition = FastActivator.CreateInstance(execution.Context.HandlerType) as IProjectionDefinition;
            if (projectionDefinition != null)
            {
                if (execution.Context.Message.Payload is IEvent)
                {
                    // infrastructure work
                    var projectionId = projectionDefinition.GetProjectionId(execution.Context.Message.Payload as IEvent);
                    var snapshot = snapshotStore.Load(projectionId);
                    projectionDefinition.InitializeState(snapshot.State);
                    Type projectionType = projectionDefinition.GetType();
                    var projectionCommits = projectionStore.Load(projectionType, projectionId, snapshot).Commits;


                    var revisionInfo = GetNextRevisionForProjectionCommit(projectionCommits);
                    var commit = new ProjectionCommit(projectionId, projectionType, revisionInfo.SnapshotMarker, execution.Context.Message.Payload as IEvent, cronusMessage.GetEventOrigin());
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

            var highestMarker = commitsSinceLastSnapshot.Any()
                ? commitsSinceLastSnapshot.Max(x => x.SnapshotMarker)
                : 1;
            var numberOfCommitsWithHighestMarker = commitsSinceLastSnapshot.Count(x => x.SnapshotMarker == highestMarker);

            var nextRevision = numberOfCommitsWithHighestMarker > numberofEventsWhenSnapshot
                ? highestMarker + 1
                : highestMarker;

            return new RevisionInfo()
            {
                SnapshotMarker = nextRevision,
                NextSnapshotRevision = nextRevision - 1
            };
        }

        class RevisionInfo
        {
            public int SnapshotMarker { get; set; }
            public int NextSnapshotRevision { get; set; }
            public bool ShouldSnapshot { get; set; }
        }
    }
}
