using System;
using System.Linq;
using Cassandra;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Configuration;

namespace Elders.Cronus.Projections.Cassandra
{
    public interface ICassandraProvider
    {
        Cluster GetCluster();
        ISession GetSession();
        ISession GetSchemaSession();
    }

    public class CassandraProvider : ICassandraProvider
    {
        private Cluster cluster;

        private readonly string connectionString;

        private readonly string defaultKeyspace;
        private readonly CronusContext context;
        private readonly ICassandraReplicationStrategy replicationStrategy;

        public CassandraProvider(IConfiguration configuration, CronusContext context, ICassandraReplicationStrategy replicationStrategy)
        {
            if (configuration is null) throw new ArgumentNullException(nameof(configuration));
            if (context is null) throw new ArgumentNullException(nameof(context));
            if (replicationStrategy is null) throw new ArgumentNullException(nameof(replicationStrategy));

            string connectionString = configuration["cronus_projections_cassandra_connectionstring"];
            if (string.IsNullOrEmpty(connectionString)) throw new ArgumentException("Missing setting: cronus_projections_cassandra_connectionstring");

            var builder = new CassandraConnectionStringBuilder(connectionString);
            if (string.IsNullOrWhiteSpace(builder.DefaultKeyspace) == false)
            {
                this.connectionString = connectionString.Replace(builder.DefaultKeyspace, "");
                this.defaultKeyspace = builder.DefaultKeyspace;
            }
            else
            {
                this.connectionString = connectionString;
            }

            this.context = context;
            this.replicationStrategy = replicationStrategy;
        }

        public Cluster GetCluster()
        {
            if (cluster is null)
            {
                cluster = Cluster
                    .Builder()
                    .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                    .WithRetryPolicy(new NoHintedHandOffRetryPolicy())
                    .WithConnectionString(connectionString)
                    .Build();
            }

            return cluster;
        }

        string GetKeyspace()
        {
            string tenantPrefix = string.IsNullOrEmpty(context.Tenant) ? string.Empty : $"{context.Tenant}_";
            var keyspace = $"{tenantPrefix}{defaultKeyspace}";
            if (keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {keyspace}");

            return keyspace;
        }

        public ISession GetSession()
        {
            ISession session = GetCluster().Connect();
            session.CreateKeyspace(replicationStrategy, GetKeyspace());

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
                    schemaSession.CreateKeyspace(replicationStrategy, GetKeyspace());
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
