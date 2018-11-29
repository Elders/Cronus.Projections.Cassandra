using Cassandra;

namespace Elders.Cronus.Projections.Cassandra
{
    public interface ICassandraProvider
    {
        Cluster GetCluster();
        ISession GetSession();
    }
}
