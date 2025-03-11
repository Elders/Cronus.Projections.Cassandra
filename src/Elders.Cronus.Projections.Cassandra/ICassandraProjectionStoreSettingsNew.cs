using Elders.Cronus.Projections.Cassandra.Infrastructure;

namespace Elders.Cronus.Projections.Cassandra;

public interface ICassandraProjectionStoreSettingsNew
{
    ProjectionsProvider ProjectionsProvider { get; }
    ICassandraProvider CassandraProvider { get; }
    IProjectionPartionsStore Partititons { get; }
    ISerializer Serializer { get; }
    VersionedProjectionsNaming ProjectionsNamingStrategy { get; }
}
