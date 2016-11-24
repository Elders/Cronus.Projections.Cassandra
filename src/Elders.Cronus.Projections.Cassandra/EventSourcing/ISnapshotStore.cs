using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface ISnapshotStore
    {
        ISnapshot Load(IBlobId id);

        void Save(IBlobId id, ISnapshot snapshot);
    }
}
