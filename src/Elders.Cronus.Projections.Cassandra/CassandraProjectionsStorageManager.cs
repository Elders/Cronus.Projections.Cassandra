using Cassandra;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.DomainModeling.Projections;
using Elders.Cronus.Projections.Cassandra.ReplicationStrategies;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionsStorageManager
    {
        private const string CreateKeyValueDataColumnFamilyTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, data blob, PRIMARY KEY (id)) WITH compression = {{ 'sstable_compression' : '' }};";
        private const string CreateColumnFamilyTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, iid text, data blob, PRIMARY KEY (id,iid)) WITH compression = {{ 'sstable_compression' : '' }};";

        private readonly ISession session;
        private readonly string keyspace;
        private readonly ICassandraReplicationStrategy replicationStrategy;
        private readonly IEnumerable<Type> projectionTypes;

        public CassandraProjectionsStorageManager(ISession session, string keyspace, ICassandraReplicationStrategy replicationStrategy, IEnumerable<Type> projectionTypes)
        {
            this.session = session;
            this.keyspace = keyspace;
            this.replicationStrategy = replicationStrategy;
            this.projectionTypes = projectionTypes;
        }

        public void CreateStorage()
        {
            var createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(keyspace);
            session.Execute(createKeySpaceQuery);
            session.ChangeKeyspace(keyspace);

            foreach (var projType in projectionTypes.Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IDataTransferObject<>))))
            {
                session.Execute(string.Format(CreateKeyValueDataColumnFamilyTemplate, projType.GetColumnFamily()).ToLowerInvariant());
            }

            foreach (var projType in projectionTypes.Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(ICollectionDataTransferObjectItem<,>))))
            {
                session.Execute(string.Format(CreateColumnFamilyTemplate, projType.GetColumnFamily()).ToLowerInvariant());
            }
        }
    }

    public static class CassandraProjectionsStorageManagerExtentions
    {
        public static string GetColumnFamily(this Type projectionType)
        {
            return projectionType.GetContractId().Replace("-", "").ToLowerInvariant();
        }
    }
}
