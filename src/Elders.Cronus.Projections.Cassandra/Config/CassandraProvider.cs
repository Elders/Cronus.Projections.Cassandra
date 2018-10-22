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

        private DataStaxCassandra.ISession session;

        private readonly string _connectionString;

        private readonly string _defaultKeyspace;
        private readonly CronusContext context;

        public string Keyspace { get; private set; }

        public CassandraProvider(IConfiguration configuration, CronusContext context)
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

        public ISession GetSession()
        {
            string tenantPrefix = string.IsNullOrEmpty(context.Tenant) ? string.Empty : $"{context.Tenant}_";
            Keyspace = $"{tenantPrefix}{_defaultKeyspace}";
            if (Keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {Keyspace}");

            ISession session = GetCluster().Connect(); // TODO Should we use GetLiveSchemaSession();
            session.CreateKeyspace(new SimpleReplicationStrategy(1), Keyspace);

            return session;
        }

        public ISession GetLiveSchemaSession()
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
                    .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                    .WithRetryPolicy(new NoHintedHandOffRetryPolicy())
                    .AddContactPoint(schemaCreatorVoltron.Address)
                    .Build();

                try
                {
                    schemaSession = schemaCluster.Connect(Keyspace);
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
