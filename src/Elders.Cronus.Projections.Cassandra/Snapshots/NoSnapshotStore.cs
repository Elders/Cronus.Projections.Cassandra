using Elders.Cronus.DomainModeling;
using System;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public class NoSnapshotStore : ISnapshotStore
    {
        public ISnapshot Load(Type projectionType, IBlobId id)
        {
            return new NoSnapshot(id, projectionType);
        }

        public void Save(ISnapshot snapshot)
        {

        }
    }
}
