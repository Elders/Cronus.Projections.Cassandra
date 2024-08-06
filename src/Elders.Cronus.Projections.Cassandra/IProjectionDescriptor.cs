using System;

namespace Elders.Cronus.Projections.Cassandra
{
    public interface IProjectionDescriptor
    {
        IComparable<long> GetPartition(IEvent @event);
    }
}
