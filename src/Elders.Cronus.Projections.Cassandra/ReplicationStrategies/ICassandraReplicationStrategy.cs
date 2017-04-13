namespace Elders.Cronus.Projections.Cassandra.ReplicationStrategies
{
    public interface ICassandraReplicationStrategy
    {
        string CreateKeySpaceTemplate(string keySpace);
    }
}
