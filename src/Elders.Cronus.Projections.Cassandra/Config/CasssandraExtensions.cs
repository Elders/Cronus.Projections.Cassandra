using System;
using Elders.Cronus.Projections.Cassandra.ReplicationStrategies;
using DataStaxCassandra = Cassandra;
using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.Config
{
    public static class CasssandraExtensions
    {
        public static string GetColumnFamily(this Type projectionType, string sufix = "")
        {
            return projectionType.GetContractId().Replace("-", "").ToLower() + sufix;
        }

        internal static void CreateKeyspace(this DataStaxCassandra.ISession session, ICassandraReplicationStrategy replicationStrategy, string keyspace)
        {
            var createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(keyspace);
            session.Execute(createKeySpaceQuery);
            session.ChangeKeyspace(keyspace);
        }
    }
}
