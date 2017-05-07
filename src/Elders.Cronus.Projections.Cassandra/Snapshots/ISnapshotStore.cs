using Elders.Cronus.DomainModeling;
using System;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public interface ISnapshotStore
    {
        ISnapshot Load(Type projectionType, IBlobId id);

        void Save(ISnapshot snapshot);
    }
}
