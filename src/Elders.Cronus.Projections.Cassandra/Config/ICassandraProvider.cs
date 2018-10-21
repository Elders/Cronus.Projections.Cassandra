using Cassandra;

namespace Elders.Cronus.Projections.Cassandra.Config
{
    public interface ICassandraProvider
    {
        string Keyspace { get; }

        Cluster GetCluster();
        ISession GetLiveSchemaSession();
        ISession GetSession();
    }
}