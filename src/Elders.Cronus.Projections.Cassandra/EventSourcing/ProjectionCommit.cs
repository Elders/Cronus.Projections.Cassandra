using System;
using Elders.Cronus.DomainModeling;
using System.Runtime.Serialization;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    [DataContract(Name = "e2a4be5e-578c-4448-bae8-43a56efa4f8e")]
    public class ProjectionCommit
    {
        ProjectionCommit() { }

        public ProjectionCommit(IBlobId projectionId, Type projectionType, IEvent @event, int snapshotMarker, EventOrigin eventOrigin, DateTime timeStamp)
        {
            ProjectionId = projectionId;
            ProjectionType = projectionType;
            Event = @event;
            SnapshotMarker = snapshotMarker;
            EventOrigin = eventOrigin;
            TimeStamp = timeStamp;
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

        [DataMember(Order = 6)]
        public DateTime TimeStamp { get; set; }
    }
}
