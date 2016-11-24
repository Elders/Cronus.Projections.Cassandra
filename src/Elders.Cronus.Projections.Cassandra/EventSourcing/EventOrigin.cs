using System;
using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class EventOrigin
    {
        EventOrigin() { }

        public EventOrigin(IAggregateRootId aggregateRootId, int aggregateRevision, int aggregateEventPosition)
        {
            if (ReferenceEquals(null, aggregateRootId)) throw new ArgumentNullException(nameof(aggregateRootId));
            if (aggregateRevision <= 0) throw new ArgumentException("Invalid revision", nameof(aggregateRevision));
            if (aggregateEventPosition < 0) throw new ArgumentException("Invalid event position", nameof(aggregateEventPosition));

            AggregateRootId = aggregateRootId;
            AggregateRevision = aggregateRevision;
            AggregateEventPosition = aggregateEventPosition;
        }

        public IAggregateRootId AggregateRootId { get; private set; }

        public int AggregateRevision { get; private set; }

        /// <summary>
        /// This is the position of the event insite a specific <see cref="AggregateRevision"/>
        /// </summary>
        public int AggregateEventPosition { get; private set; }
    }
}
