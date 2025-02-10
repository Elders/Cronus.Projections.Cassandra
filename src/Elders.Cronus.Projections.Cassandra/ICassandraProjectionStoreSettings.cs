using Elders.Cronus.Projections.Cassandra.Infrastructure;
using System;

namespace Elders.Cronus.Projections.Cassandra
{
    [Obsolete("Will be removed in v12")]
    public interface ICassandraProjectionStoreSettings
    {
        ProjectionsProvider ProjectionsProvider { get; }
        ICassandraProvider CassandraProvider { get; }
        ISerializer Serializer { get; }
        VersionedProjectionsNaming ProjectionsNamingStrategy { get; }
    }
}
