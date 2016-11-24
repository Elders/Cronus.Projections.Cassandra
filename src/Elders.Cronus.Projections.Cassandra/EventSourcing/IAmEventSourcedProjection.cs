using Elders.Cronus.DomainModeling;
using System.Collections.Generic;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IAmEventSourcedProjection
    {
        void ReplayEvents(IEnumerable<IEvent> events);
    }
}
