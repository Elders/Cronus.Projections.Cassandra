using System;

namespace Elders.Cronus.Projections.Cassandra
{
    public interface IProjectionDescriptor /// TODO: use it to extend <see cref="CassandraProjectionStoreNew.CalculatePartition"/>
    {
        IComparable<long> GetPartition(IEvent @event);
    }
}
