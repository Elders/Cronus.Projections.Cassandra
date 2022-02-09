using System;
using Cassandra;
using Microsoft.Extensions.Options;
using DataStax = Cassandra;

namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public class CassandraProvider : ICassandraProvider
    {
        private bool optionsHasChanged = true;

        protected CassandraProviderOptions options;

        protected readonly IKeyspaceNamingStrategy keyspaceNamingStrategy;
        protected readonly ICassandraReplicationStrategy replicationStrategy;
        protected readonly IInitializer initializer;

        protected ICluster cluster;
        protected ISession session;

        private static object establishNewConnection = new object();

        private string baseConfigurationKeyspace;

        public CassandraProvider(IOptionsMonitor<CassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ICassandraReplicationStrategy replicationStrategy, IInitializer initializer = null)
        {
            if (optionsMonitor is null) throw new ArgumentNullException(nameof(optionsMonitor));
            if (keyspaceNamingStrategy is null) throw new ArgumentNullException(nameof(keyspaceNamingStrategy));
            if (replicationStrategy is null) throw new ArgumentNullException(nameof(replicationStrategy));

            options = optionsMonitor.CurrentValue;
            optionsMonitor.OnChange(Changed);

            this.keyspaceNamingStrategy = keyspaceNamingStrategy;
            this.replicationStrategy = replicationStrategy;
            this.initializer = initializer;
        }

        public ICluster GetCluster()
        {
            if (cluster is null == false && optionsHasChanged == false)
                return cluster;

            Builder builder = initializer as Builder;
            if (builder is null)
            {
                builder = DataStax.Cluster.Builder();
                //  TODO: check inside the `cfg` (var cfg = builder.GetConfiguration();) if we already have connectionString specified

                string connectionString = options.ConnectionString;

                var hackyBuilder = new CassandraConnectionStringBuilder(connectionString);
                if (string.IsNullOrEmpty(hackyBuilder.DefaultKeyspace) == false)
                    connectionString = connectionString.Replace(hackyBuilder.DefaultKeyspace, string.Empty);
                baseConfigurationKeyspace = hackyBuilder.DefaultKeyspace;

                var connStrBuilder = new CassandraConnectionStringBuilder(connectionString);

                cluster?.Shutdown(30000);
                cluster = connStrBuilder
                    .ApplyToBuilder(builder)
                    .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                    .WithRetryPolicy(new NoHintedHandOffRetryPolicy())
                    .Build();

                cluster.RefreshSchema();
            }

            else
            {
                cluster = DataStax.Cluster.BuildFrom(initializer);
            }

            optionsHasChanged = false;

            return cluster;
        }

        protected virtual string GetKeyspace()
        {
            return keyspaceNamingStrategy.GetName(baseConfigurationKeyspace).ToLower();
        }

        public ISession GetSession()
        {
            if (session is null || session.IsDisposed || optionsHasChanged)
            {
                lock (establishNewConnection)
                {
                    if (session is null || session.IsDisposed || optionsHasChanged)
                    {
                        if (optionsHasChanged)
                            session?.Dispose();

                        try
                        {
                            session = GetCluster().Connect(GetKeyspace());
                        }
                        catch (InvalidQueryException)
                        {
                            using (ISession schemaSession = GetCluster().Connect())
                            {
                                var createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(GetKeyspace());
                                schemaSession.Execute(createKeySpaceQuery);
                            }

                            session = GetCluster().Connect(GetKeyspace());
                        }
                    }
                }
            }

            return session;
        }

        private void Changed(CassandraProviderOptions newOptions)
        {
            if (options != newOptions)
            {
                options = newOptions;
                optionsHasChanged = true;
            }
        }
    }
}
