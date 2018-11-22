namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStoreSettings : ICassandraProjectionStoreSettings
    {
        public CassandraProjectionStoreSettings(ICassandraProvider cassandraProvider, ISerializer serializer, IProjectionsNamingStrategy projectionsNamingStrategy, ProjectionsProvider projectionsProvider)
        {
            CassandraProvider = cassandraProvider;
            Serializer = serializer;
            ProjectionsNamingStrategy = projectionsNamingStrategy;
            ProjectionsProvider = projectionsProvider;
        }

        public ICassandraProvider CassandraProvider { get; }
        public ISerializer Serializer { get; }
        public IProjectionsNamingStrategy ProjectionsNamingStrategy { get; }

        public ProjectionsProvider ProjectionsProvider { get; }
    }
}
