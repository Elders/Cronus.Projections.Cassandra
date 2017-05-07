using Elders.Cronus.DomainModeling;
using System;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public class NoSnapshot : ISnapshot
    {
        public NoSnapshot(IBlobId id, Type projectionType)
        {
            Id = id;
            ProjectionType = projectionType;
        }

        public IBlobId Id { get; set; }

        public Type ProjectionType { get; set; }

        public int Revision { get { return 0; } }

        public object State { get; private set; }

        public void InitializeState(object state)
        {

        }
    }
}
