using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public class NoSnapshotStore : ISnapshotStore
    {
        public ISnapshot Load(string projectionContractId, IBlobId id)
        {
            return new NoSnapshot(id, projectionContractId);
        }

        public void Save(ISnapshot snapshot, bool isReplay)
        {

        }
    }
}
