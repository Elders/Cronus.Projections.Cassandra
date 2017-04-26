using System;
using Elders.Cronus.DomainModeling;
using System.Runtime.Serialization;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    [DataContract(Name = "ed0d9b4e-3ac5-4cd4-9598-7bf5687b037a")]
    public class ProjectionCommit
    {
        ProjectionCommit() { }

        public ProjectionCommit(IBlobId projectionId, Type projectionType, int snapshotMarker, IEvent @event, EventOrigin @eventOrigin)
        {
            SnapshotMarker = snapshotMarker;
            ProjectionType = projectionType;
            ProjectionId = projectionId;
            Event = @event;
            EventOrigin = eventOrigin;
        }

        [DataMember(Order = 1)]
        public IBlobId ProjectionId { get; private set; }

        [DataMember(Order = 2)]
        public Type ProjectionType { get; set; }

        [DataMember(Order = 3)]
        public IEvent Event { get; private set; }

        [DataMember(Order = 4)]
        public int SnapshotMarker { get; private set; }

        [DataMember(Order = 5)]
        public EventOrigin EventOrigin { get; set; }
    }
}
