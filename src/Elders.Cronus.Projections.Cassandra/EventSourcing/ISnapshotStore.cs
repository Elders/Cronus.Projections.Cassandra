using System;
using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface ISnapshotStore
    {
        ISnapshot Load(IBlobId id);

        void Save(IBlobId id, ISnapshot snapshot);
    }

    public class NoSnapshotStore : ISnapshotStore
    {
        public ISnapshot Load(IBlobId id)
        {
            return new NoSnapshot();
        }

        public void Save(IBlobId id, ISnapshot snapshot)
        {

        }
    }

    public class NoSnapshot : ISnapshot
    {
        public int Revision { get { return 0; } }

        public object State { get; set; }

        public void InitializeState(object state)
        {

        }
    }
}
