using Cassandra;

namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public interface ICassandraProvider
    {
        ICluster GetCluster();
        ISession GetSession();
    }
}
