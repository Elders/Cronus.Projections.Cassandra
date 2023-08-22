using System.Threading.Tasks;

namespace Elders.Cronus.Projections.Cassandra
{
    public interface ICassandraSnapshotStoreSchema
    {
        Task CreateSnapshotStorageAsync(string location);
        Task CreateTableAsync(string location);
        Task DropTableAsync(string location);
        Task<string> GetKeypaceAsync();
    }
}