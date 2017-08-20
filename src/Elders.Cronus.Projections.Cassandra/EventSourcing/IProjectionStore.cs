using Elders.Cronus.DomainModeling;
using Elders.Cronus.Projections.Cassandra.Snapshots;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IProjectionStore
    {
        ProjectionStream Load(string contractId, IBlobId projectionId, ISnapshot snapshot, bool isReplay);

        void Save(ProjectionCommit commit, bool isReplay);

        void BeginReplay(string projectionContractId);

        void EndReplay(string projectionContractId);
    }
}
