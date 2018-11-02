using System;
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
    }

    public class CassandraProvider : ICassandraProvider
    {
        public const string ConnectionStringSettingKey = "cronus_projections_cassandra_connectionstring";

        protected Cluster cluster;
        protected ISession session;

        private string baseConfigurationKeyspace;
        private readonly IConfiguration configuration;
        private readonly CronusContext context;
        private readonly ICassandraReplicationStrategy replicationStrategy;
        private readonly IInitializer initializer;

        public CassandraProvider(IConfiguration configuration, CronusContext context, ICassandraReplicationStrategy replicationStrategy, IInitializer initializer = null)
        {
            if (configuration is null) throw new ArgumentNullException(nameof(configuration));
            if (context is null) throw new ArgumentNullException(nameof(context));
            if (replicationStrategy is null) throw new ArgumentNullException(nameof(replicationStrategy));

            this.configuration = configuration;
            this.context = context;
            this.replicationStrategy = replicationStrategy;
            this.initializer = initializer;
        }

        public Cluster GetCluster()
        {
            if (cluster is null == false)
                return cluster;

            Builder builder = initializer as Builder;
            if (builder is null)
            {
                builder = Cluster.Builder();
                //  TODO: check inside the `cfg` (var cfg = builder.GetConfiguration();) if we already have connectionString specified

                string connectionString = configuration.GetRequired(ConnectionStringSettingKey);

                var hackyBuilder = new CassandraConnectionStringBuilder(connectionString);
                if (string.IsNullOrEmpty(hackyBuilder.DefaultKeyspace) == false)
                    connectionString = connectionString.Replace(hackyBuilder.DefaultKeyspace, "");
                baseConfigurationKeyspace = hackyBuilder.DefaultKeyspace;

                var connStrBuilder = new CassandraConnectionStringBuilder(connectionString);
                cluster = connStrBuilder
                    .ApplyToBuilder(builder)
                    .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                    .WithRetryPolicy(new NoHintedHandOffRetryPolicy())
                    .Build();
            }

            else
            {
                cluster = Cluster.BuildFrom(initializer);
            }

            return cluster;
        }

        protected virtual string GetKeyspace()
        {
            string tenantPrefix = string.IsNullOrEmpty(context.Tenant) ? string.Empty : $"{context.Tenant}_";
            var keyspace = $"{tenantPrefix}{baseConfigurationKeyspace}";
            if (keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {keyspace}");

            return keyspace;
        }

        public ISession GetSession()
        {
            if (session is null)
            {
                session = GetCluster().Connect();
                CreateKeyspace(replicationStrategy, GetKeyspace());
            }

            return session;
        }

        private void CreateKeyspace(ICassandraReplicationStrategy replicationStrategy, string keyspace)
        {
            var createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(keyspace);
            session.Execute(createKeySpaceQuery);
            session.ChangeKeyspace(keyspace);
        }
    }
}
