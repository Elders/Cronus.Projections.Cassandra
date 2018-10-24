using System;
using Elders.Cronus.Projections.Cassandra.ReplicationStrategies;
using DataStaxCassandra = Cassandra;
using Cassandra;
using Microsoft.Extensions.Configuration;
using System.Linq;
using Elders.Cronus.MessageProcessing;

namespace Elders.Cronus.Projections.Cassandra.Config
{
    public class CassandraProvider : ICassandraProvider
    {
        private DataStaxCassandra.Cluster cluster;

        private readonly string _connectionString;

        private readonly string _defaultKeyspace;
        private readonly CronusContext context;
        private readonly ICassandraReplicationStrategy replicationStrategy;

        public CassandraProvider(IConfiguration configuration, CronusContext context, ICassandraReplicationStrategy replicationStrategy)
        {
            if (configuration is null) throw new ArgumentNullException(nameof(configuration));

            string connectionString = configuration["cronus_projections_cassandra_connectionstring"];
            var builder = new DataStaxCassandra.CassandraConnectionStringBuilder(connectionString);
            if (string.IsNullOrWhiteSpace(builder.DefaultKeyspace) == false)
            {
                _connectionString = connectionString.Replace(builder.DefaultKeyspace, "");
                _defaultKeyspace = builder.DefaultKeyspace;
            }
            else
            {
                this._connectionString = connectionString;
            }

            this.context = context;
            this.replicationStrategy = replicationStrategy;
        }

        public DataStaxCassandra.Cluster GetCluster()
        {
            if (cluster is null)
            {
                cluster = DataStaxCassandra.Cluster
                    .Builder()
                    .WithReconnectionPolicy(new DataStaxCassandra.ExponentialReconnectionPolicy(100, 100000))
                    .WithRetryPolicy(new NoHintedHandOffRetryPolicy())
                    .WithConnectionString(_connectionString)
                    .Build();
            }

            return cluster;
        }

        string GetKeyspace()
        {
            string tenantPrefix = string.IsNullOrEmpty(context.Tenant) ? string.Empty : $"{context.Tenant}_";
            var keyspace = $"{tenantPrefix}{_defaultKeyspace}";
            if (keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {keyspace}");

            return keyspace;
        }

        public ISession GetSession()
        {
            ISession session = GetCluster().Connect();
            session.CreateKeyspace(new SimpleReplicationStrategy(1), GetKeyspace());

            return session;
        }

        public ISession GetSchemaSession()
        {
            var hosts = GetCluster().AllHosts().ToList();
            ISession schemaSession = null;
            var counter = 0;

            while (ReferenceEquals(null, schemaSession))
            {
                var schemaCreatorVoltron = hosts.ElementAtOrDefault(counter++);
                if (ReferenceEquals(null, schemaCreatorVoltron))
                    throw new InvalidOperationException($"Could not find a Cassandra node! Hosts: '{string.Join(", ", hosts.Select(x => x.Address))}'");

                var schemaCluster = Cluster
                    .Builder()
                    .AddContactPoint(schemaCreatorVoltron.Address)
                    .Build();

                try
                {
                    schemaSession = schemaCluster.Connect();
                    schemaSession.CreateKeyspace(new SimpleReplicationStrategy(1), GetKeyspace());
                }
                catch (NoHostAvailableException)
                {
                    if (counter < hosts.Count)
                        continue;
                    else
                        throw;
                }
            }

            return schemaSession;
        }
    }
}
