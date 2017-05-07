using Elders.Cronus.DomainModeling;
using Elders.Cronus.Projections.Cassandra.Snapshots;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IProjectionDefinition : IHaveState, IAmEventSourcedProjection
    {
        IBlobId GetProjectionId(IEvent @event);

        void Apply(IEvent @event);
    }
}
