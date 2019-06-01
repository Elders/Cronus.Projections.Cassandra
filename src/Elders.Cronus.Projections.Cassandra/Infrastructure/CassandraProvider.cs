using System;
using Cassandra;

namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public class CassandraProvider : ICassandraProvider
    {
        protected CassandraProviderOptions options;

        protected readonly IKeyspaceNamingStrategy keyspaceNamingStrategy;
        protected readonly ICassandraReplicationStrategy replicationStrategy;
        protected readonly ICassandraInitializerProvider initializerProvider;

        protected Cluster cluster;
        protected ISession session;

        public CassandraProvider(ICassandraInitializerProvider initializerProvider, IKeyspaceNamingStrategy keyspaceNamingStrategy, ICassandraReplicationStrategy replicationStrategy)
        {
            if (initializerProvider is null) throw new ArgumentNullException(nameof(initializerProvider));
            if (keyspaceNamingStrategy is null) throw new ArgumentNullException(nameof(keyspaceNamingStrategy));
            if (replicationStrategy is null) throw new ArgumentNullException(nameof(replicationStrategy));

            this.initializerProvider = initializerProvider;
            this.keyspaceNamingStrategy = keyspaceNamingStrategy;
            this.replicationStrategy = replicationStrategy;
        }

        public Cluster GetCluster()
        {
            if (cluster is null || initializerProvider.ConfigurationHasChanged)
            {
                cluster?.Shutdown(30000);

                IInitializer initializer = initializerProvider.GetInitializer();
                cluster = Cluster.BuildFrom(initializer);
            }

            return cluster;
        }

        protected virtual string GetKeyspace()
        {
            return keyspaceNamingStrategy.GetName(cluster.Configuration.ClientOptions.DefaultKeyspace).ToLower();
        }

        public ISession GetSession()
        {
            if (session is null || session.IsDisposed || initializerProvider.ConfigurationHasChanged)
            {
                session?.Dispose();
                session = GetCluster().Connect();
                CreateKeyspace(GetKeyspace(), replicationStrategy);
            }

            return session;
        }

        private void CreateKeyspace(string keyspace, ICassandraReplicationStrategy replicationStrategy)
        {
            var createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(keyspace);
            session.Execute(createKeySpaceQuery);
            session.ChangeKeyspace(keyspace);
        }
    }
}
