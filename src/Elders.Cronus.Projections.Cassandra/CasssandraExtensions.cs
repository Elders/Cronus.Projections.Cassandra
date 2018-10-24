using System;
using Cassandra;
using Elders.Cronus.Projections.Cassandra.ReplicationStrategies;

namespace Elders.Cronus.Projections.Cassandra
{
    static class CasssandraExtensions
    {
        internal static string GetColumnFamily(this Type projectionType, string sufix = "")
        {
            return projectionType.GetContractId().Replace("-", "").ToLower() + sufix;
        }

        internal static string GetColumnFamily(this string contractId, string sufix = "")
        {
            return contractId.Replace("-", "").ToLower() + sufix;
        }

        internal static string GetColumnFamily(this string contractId, ProjectionVersion version)
        {
            return contractId.Replace("-", "").ToLower() + "_" + version.Hash + "_" + version.Revision;
        }

        internal static void CreateKeyspace(this ISession session, ICassandraReplicationStrategy replicationStrategy, string keyspace)
        {
            var createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(keyspace);
            session.Execute(createKeySpaceQuery);
            session.ChangeKeyspace(keyspace);
        }
    }
}
