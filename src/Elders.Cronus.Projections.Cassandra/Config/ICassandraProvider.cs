using Cassandra;

namespace Elders.Cronus.Projections.Cassandra.Config
{
    public interface ICassandraProvider
    {
        Cluster GetCluster();
        ISession GetSchemaSession();
        ISession GetSession();
    }
}
