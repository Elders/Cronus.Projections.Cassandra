using Cassandra;

namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public interface ICassandraProvider
    {
        Cluster GetCluster();
        ISession GetSession();
    }
}
