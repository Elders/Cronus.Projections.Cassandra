using Elders.Cronus.DomainModeling;
using Elders.Cronus.Projections.Cassandra.Snapshots;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IProjectionStore
    {
        ProjectionStream Load(string contractId, IBlobId projectionId, ISnapshot snapshot);

        void Save(ProjectionCommit commit);

        void BeginReplay(string projectionContractId);

        void EndReplay(string projectionContractId);
    }
}
