using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IProjectionRepository
    {
        IProjectionGetResult<T> Get<T>(IBlobId projectionId) where T : IProjectionDefinition;
    }
}
