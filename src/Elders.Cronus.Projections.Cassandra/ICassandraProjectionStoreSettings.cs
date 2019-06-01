using Elders.Cronus.Projections.Cassandra.Infrastructure;

namespace Elders.Cronus.Projections.Cassandra
{
    public interface ICassandraProjectionStoreSettings
    {
        ProjectionsProvider ProjectionsProvider { get; }
        ICassandraProvider CassandraProvider { get; }
        ISerializer Serializer { get; }
        IProjectionsNamingStrategy ProjectionsNamingStrategy { get; }
    }
}
