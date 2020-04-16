namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public interface ICassandraReplicationStrategy
    {
        string CreateKeySpaceTemplate(string keySpace);
    }
}
