using Elders.Cronus.DomainModeling;
using System;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface ISnapshotStore
    {
        ISnapshot Load(Type projectionType, IBlobId id);

        void Save(IBlobId id, ISnapshot snapshot);
    }

    public class NoSnapshotStore : ISnapshotStore
    {
        public ISnapshot Load(Type projectionType, IBlobId id)
        {
            return new NoSnapshot(id, projectionType);
        }

        public void Save(IBlobId id, ISnapshot snapshot)
        {

        }
    }

    public class NoSnapshot : ISnapshot
    {
        public NoSnapshot(IBlobId id, Type projectionType)
        {
            Id = id;
            ProjectionType = projectionType;
        }

        public IBlobId Id { get; set; }
        public Type ProjectionType { get; set; }

        public int Revision { get { return 1; } }

        public object State { get; set; }

        public void InitializeState(object state)
        {

        }
    }
}
