namespace Elders.Cronus.Projections.Cassandra;

public interface IKeyspaceNamingStrategy
{
    string GetName(string baseConfigurationKeyspace);
}
