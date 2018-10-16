using System;
using Elders.Cronus.Projections.Cassandra.ReplicationStrategies;
using DataStaxCassandra = Cassandra;
using Cassandra;
using Microsoft.Extensions.Configuration;

namespace Elders.Cronus.Projections.Cassandra.Config
{
    public class CassandraProvider
    {
        private DataStaxCassandra.Cluster cluster;

        private DataStaxCassandra.ISession session;

        private readonly string _connectionString;

        private readonly string _defaultKeyspace;

        public string Keyspace { get { return _defaultKeyspace; } }

        public CassandraProvider(IConfiguration configuration)
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
            if (session is null)
            {
                session = GetCluster().Connect();
                session.CreateKeyspace(new SimpleReplicationStrategy(1), _defaultKeyspace);
            }

            return session;
        }
    }
}
