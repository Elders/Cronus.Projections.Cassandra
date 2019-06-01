using Cassandra;

namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public interface ICassandraInitializerProvider
    {
        IInitializer GetInitializer();
        bool ConfigurationHasChanged { get; }
    }
}
