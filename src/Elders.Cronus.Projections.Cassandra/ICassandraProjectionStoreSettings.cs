namespace Elders.Cronus.Projections.Cassandra
{
    public interface ICassandraProjectionStoreSettings
    {
        ICassandraProvider CassandraProvider { get; }
        ISerializer Serializer { get; }
        IProjectionsNamingStrategy ProjectionsNamingStrategy { get; }
    }
}
