namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStoreSettings : ICassandraProjectionStoreSettings
    {
        public CassandraProjectionStoreSettings(ICassandraProvider cassandraProvider, ISerializer serializer, IProjectionsNamingStrategy projectionsNamingStrategy)
        {
            CassandraProvider = cassandraProvider;
            Serializer = serializer;
            ProjectionsNamingStrategy = projectionsNamingStrategy;
        }

        public ICassandraProvider CassandraProvider { get; }
        public ISerializer Serializer { get; }
        public IProjectionsNamingStrategy ProjectionsNamingStrategy { get; }
    }
}
