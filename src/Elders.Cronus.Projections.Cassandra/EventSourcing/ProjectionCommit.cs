using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class ProjectionCommit
    {
        public ProjectionCommit(IBlobId projectionId, int snapshotMarker, IEvent @event, EventOrigin @eventOrigin)
        {
            SnapshotMarker = snapshotMarker;
            ProjectionId = projectionId;
            Event = @event;
            EventOrigin = eventOrigin;
        }

        public IBlobId ProjectionId { get; private set; }

        public int SnapshotMarker { get; private set; }

        public IEvent Event { get; private set; }

        public EventOrigin EventOrigin { get; set; }
    }
}
