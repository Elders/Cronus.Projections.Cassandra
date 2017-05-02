using System;
using Elders.Cronus.IocContainer;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Projections.Cassandra.ReplicationStrategies;
using DataStaxCassandra = Cassandra;
using System.Collections.Generic;
using Elders.Cronus.DomainModeling.Projections;
using Elders.Cronus.Projections.Cassandra.EventSourcing;
using Elders.Cronus.Serializer;
using System.Reflection;
using System.Linq;
using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.Config
{
    public static class CassandraProjectionsExtensions
    {
        public static T ConfigureCassandraProjectionsStore<T>(this T self, Action<CassandraProjectionsStoreSettings> configure) where T : ISettingsBuilder
        {
            CassandraProjectionsStoreSettings settings = new CassandraProjectionsStoreSettings(self);
            settings.SetProjectionsReconnectionPolicy(new DataStaxCassandra.ExponentialReconnectionPolicy(100, 100000));
            settings.SetProjectionsRetryPolicy(new NoHintedHandOffRetryPolicy());
            settings.SetProjectionsReplicationStrategy(new SimpleReplicationStrategy(1));
            settings.SetProjectionsWriteConsistencyLevel(DataStaxCassandra.ConsistencyLevel.All);
            settings.SetProjectionsReadConsistencyLevel(DataStaxCassandra.ConsistencyLevel.Quorum);

            configure?.Invoke(settings);

            var projectionTypes = (settings as ICassandraProjectionsSettings).ProjectionTypes;

            if (ReferenceEquals(null, projectionTypes) || projectionTypes.Any() == false)
                throw new InvalidOperationException("No projection types are registerd. Please use SetProjectionTypes.");

            (settings as ISettingsBuilder).Build();
            return self;
        }

        public static T UseCassandraProjections<T>(this T self, Action<CassandraProjectionsSettings> configure) where T : ISubscrptionMiddlewareSettings
        {
            CassandraProjectionsSettings settings = new CassandraProjectionsSettings(self, self as ISubscrptionMiddlewareSettings);
            settings.SetProjectionsReconnectionPolicy(new DataStaxCassandra.ExponentialReconnectionPolicy(100, 100000));
            settings.SetProjectionsRetryPolicy(new NoHintedHandOffRetryPolicy());
            settings.SetProjectionsReplicationStrategy(new SimpleReplicationStrategy(1));
            settings.SetProjectionsWriteConsistencyLevel(DataStaxCassandra.ConsistencyLevel.All);
            settings.SetProjectionsReadConsistencyLevel(DataStaxCassandra.ConsistencyLevel.Quorum);

            (settings as ICassandraProjectionsSettings).ProjectionTypes = self.HandlerRegistrations;

            configure?.Invoke(settings);

            (settings as ISettingsBuilder).Build();
            return self;
        }

        /// <summary>
        /// Set the connection string for projections.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="connectionString">Connection string that will be used to connect to the cassandra cluster.</param>
        /// <returns></returns>
        public static T SetProjectionsConnectionString<T>(this T self, string connectionString) where T : ICassandraProjectionsSettings
        {
            var builder = new DataStaxCassandra.CassandraConnectionStringBuilder(connectionString);
            if (string.IsNullOrWhiteSpace(builder.DefaultKeyspace) == false)
            {
                self.ConnectionString = connectionString.Replace(builder.DefaultKeyspace, "");
                self.SetProjectionsKeyspace(builder.DefaultKeyspace);
            }
            else
            {
                self.ConnectionString = connectionString;
            }

            return self;
        }

        /// <summary>
        /// Set the keyspace.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="keyspace">Keyspace that will be used for the event store.</param>
        /// <returns></returns>
        public static T SetProjectionsKeyspace<T>(this T self, string keyspace) where T : ICassandraProjectionsSettings
        {
            self.Keyspace = keyspace;
            return self;
        }

        /// <summary>
        /// Use when you want to override all the default settings. You should use a connection string without the default keyspace and use the SetKeyspace method to specify it.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="cluster">Fully configured Cassandra cluster object.</param>
        /// <returns></returns>
        public static T SetProjectionsCluster<T>(this T self, DataStaxCassandra.Cluster cluster) where T : ICassandraProjectionsSettings
        {
            self.Cluster = cluster;
            return self;
        }

        /// <summary>
        /// Use to se the consistency level that is going to be used when writing to the event store.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="writeConsistencyLevel"></param>
        /// <returns></returns>
        public static T SetProjectionsWriteConsistencyLevel<T>(this T self, DataStaxCassandra.ConsistencyLevel writeConsistencyLevel) where T : ICassandraProjectionsSettings
        {
            self.WriteConsistencyLevel = writeConsistencyLevel;
            return self;
        }

        /// <summary>
        /// Use to set the consistency level that is going to be used when reading from the event store.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="readConsistencyLevel"></param>
        /// <returns></returns>
        public static T SetProjectionsReadConsistencyLevel<T>(this T self, DataStaxCassandra.ConsistencyLevel readConsistencyLevel) where T : ICassandraProjectionsSettings
        {
            self.ReadConsistencyLevel = readConsistencyLevel;
            return self;
        }

        /// <summary>
        /// Use to override the default reconnection policy.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="policy">Cassandra reconnection policy.</param>
        /// <returns></returns>
        public static T SetProjectionsReconnectionPolicy<T>(this T self, DataStaxCassandra.IReconnectionPolicy policy) where T : ICassandraProjectionsSettings
        {
            self.ReconnectionPolicy = policy;
            return self;
        }

        /// <summary>
        /// Use to override the default retry policy.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="policy">Cassandra retry policy.</param>
        /// <returns></returns>
        public static T SetProjectionsRetryPolicy<T>(this T self, DataStaxCassandra.IRetryPolicy policy) where T : ICassandraProjectionsSettings
        {
            self.RetryPolicy = policy;
            return self;
        }

        /// <summary>
        /// Use to override the default replication strategy.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="replicationStrategy">Cassandra replication strategy.</param>
        /// <returns></returns>
        public static T SetProjectionsReplicationStrategy<T>(this T self, ICassandraReplicationStrategy replicationStrategy) where T : ICassandraProjectionsSettings
        {
            self.ReplicationStrategy = replicationStrategy;
            return self;
        }

        /// <summary>
        /// Set the projection types.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="projectionsAssembley">Assembly that contains the projection types.</param>
        /// <returns></returns>
        public static T SetProjectionTypes<T>(this T self, Assembly projectionsAssembley) where T : ICassandraProjectionsSettings
        {
            return self.SetProjectionTypes(projectionsAssembley.GetExportedTypes());
        }

        /// <summary>
        /// Set the projection types.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="projectionTypes">The projection types.</param>
        /// <returns></returns>
        public static T SetProjectionTypes<T>(this T self, IEnumerable<Type> projectionTypes) where T : ICassandraProjectionsSettings
        {
            self.ProjectionTypes = projectionTypes;
            return self;
        }
    }

    public interface ICassandraProjectionsSettings : ISettingsBuilder
    {
        string Keyspace { get; set; }
        string ConnectionString { get; set; }
        IEnumerable<Type> ProjectionTypes { get; set; }
        DataStaxCassandra.Cluster Cluster { get; set; }
        DataStaxCassandra.ConsistencyLevel WriteConsistencyLevel { get; set; }
        DataStaxCassandra.ConsistencyLevel ReadConsistencyLevel { get; set; }
        DataStaxCassandra.IRetryPolicy RetryPolicy { get; set; }
        DataStaxCassandra.IReconnectionPolicy ReconnectionPolicy { get; set; }
        ICassandraReplicationStrategy ReplicationStrategy { get; set; }
    }

    public class CassandraProjectionsSettings : CassandraProjectionsStoreSettings
    {
        private ISubscrptionMiddlewareSettings subscrptionMiddlewareSettings;

        public CassandraProjectionsSettings(ISettingsBuilder settingsBuilder, ISubscrptionMiddlewareSettings subscrptionMiddlewareSettings) : base(settingsBuilder)
        {
            this.subscrptionMiddlewareSettings = subscrptionMiddlewareSettings;
        }

        public override void Build()
        {
            var builder = this as ISettingsBuilder;
            ICassandraProjectionsSettings settings = this as ICassandraProjectionsSettings;
            base.Build();
            subscrptionMiddlewareSettings.Middleware(x => { return new EventSourcedProjectionsMiddleware(builder.Container.Resolve<IProjectionStore>(), builder.Container.Resolve<ISnapshotStore>()); });
        }
    }

    public static class CasssandraExtensions
    {
        const string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, sm int, evarid text, evarrev int, evarts bigint, evarpos int, data blob, PRIMARY KEY ((id, sm), evarid, evarrev, evarpos, evarts)) WITH CLUSTERING ORDER BY (evarid ASC);";

        public static void InitializeProjectionDatabase(this DataStaxCassandra.ISession session, IEnumerable<Type> projections)
        {
            foreach (var projType in projections
                .Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x))
                .Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IEventHandler<>))))
            {
                session.Execute(string.Format(CreateProjectionEventsTableTemplate, projType.GetColumnFamily()));
            }
        }

        public static void InitializeProjectionDatabase(this DataStaxCassandra.ISession session, IEnumerable<Assembly> assemblyContainingProjections)
        {
            InitializeProjectionDatabase(session, assemblyContainingProjections.SelectMany(x => x.GetExportedTypes()));
        }

        public static string GetColumnFamily(this Type projectionType)
        {
            return projectionType.GetContractId().Replace("-", "").ToLower();
        }

        internal static void CreateKeyspace(this DataStaxCassandra.ISession session, ICassandraReplicationStrategy replicationStrategy, string keyspace)
        {
            var createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(keyspace);
            session.Execute(createKeySpaceQuery);
            session.ChangeKeyspace(keyspace);
        }
    }

    public class CassandraProjectionsStoreSettings : SettingsBuilder, ICassandraProjectionsSettings
    {
        public CassandraProjectionsStoreSettings(ISettingsBuilder settingsBuilder) : base(settingsBuilder) { }

        public override void Build()
        {
            var builder = this as ISettingsBuilder;
            ICassandraProjectionsSettings settings = this as ICassandraProjectionsSettings;

            DataStaxCassandra.Cluster cluster = null;

            if (ReferenceEquals(null, settings.Cluster))
            {
                cluster = DataStaxCassandra.Cluster
                    .Builder()
                    .WithReconnectionPolicy(settings.ReconnectionPolicy)
                    .WithRetryPolicy(settings.RetryPolicy)
                    .WithConnectionString(settings.ConnectionString)
                    .Build();
            }
            else
            {
                cluster = settings.Cluster;
            }

            var session = cluster.Connect();
            session.CreateKeyspace(settings.ReplicationStrategy, settings.Keyspace);
            session.InitializeProjectionDatabase(settings.ProjectionTypes);

            var serializer = builder.Container.Resolve<ISerializer>();

            builder.Container.RegisterSingleton<IProjectionStore>(() => new CassandraProjectionStore(session, serializer));
            builder.Container.RegisterSingleton<ISnapshotStore>(() => new NoSnapshotStore());
            builder.Container.RegisterTransient<IProjectionRepository>(() => new ProjectionRepository(builder.Container.Resolve<IProjectionStore>(), builder.Container.Resolve<ISnapshotStore>()));
        }

        string ICassandraProjectionsSettings.Keyspace { get; set; }

        string ICassandraProjectionsSettings.ConnectionString { get; set; }

        IEnumerable<Type> ICassandraProjectionsSettings.ProjectionTypes { get; set; }

        DataStaxCassandra.Cluster ICassandraProjectionsSettings.Cluster { get; set; }

        DataStaxCassandra.ConsistencyLevel ICassandraProjectionsSettings.WriteConsistencyLevel { get; set; }

        DataStaxCassandra.ConsistencyLevel ICassandraProjectionsSettings.ReadConsistencyLevel { get; set; }

        DataStaxCassandra.IRetryPolicy ICassandraProjectionsSettings.RetryPolicy { get; set; }

        DataStaxCassandra.IReconnectionPolicy ICassandraProjectionsSettings.ReconnectionPolicy { get; set; }

        ICassandraReplicationStrategy ICassandraProjectionsSettings.ReplicationStrategy { get; set; }
    }
}
