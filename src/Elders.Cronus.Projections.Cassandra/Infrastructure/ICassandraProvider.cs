using Cassandra;
using System.Threading.Tasks;

namespace Elders.Cronus.Projections.Cassandra.Infrastructure;

public interface ICassandraProvider
{
    string GetKeyspace();
    Task<ICluster> GetClusterAsync();
    Task<ISession> GetSessionAsync();
}
