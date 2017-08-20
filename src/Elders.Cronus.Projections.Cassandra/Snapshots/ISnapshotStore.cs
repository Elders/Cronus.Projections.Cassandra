using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public interface ISnapshotStore
    {
        ISnapshot Load(string projectionContractId, IBlobId id, bool isReplay);

        void Save(ISnapshot snapshot, bool isReplay);
    }
}
