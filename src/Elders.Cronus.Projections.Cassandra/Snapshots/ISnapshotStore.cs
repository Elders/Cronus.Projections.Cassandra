using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public interface ISnapshotStore
    {
        ISnapshot Load(string projectionContractId, IBlobId id);

        void Save(ISnapshot snapshot);
    }
}
