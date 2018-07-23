using System;
using Elders.Cronus.IocContainer;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Projections.Cassandra.ReplicationStrategies;
using DataStaxCassandra = Cassandra;
using System.Collections.Generic;
using Elders.Cronus.Projections.Cassandra.EventSourcing;
using Elders.Cronus.Serializer;
using System.Reflection;
using System.Linq;
using Elders.Cronus.Projections.Cassandra.Snapshots;
using Elders.Cronus.Projections.Snapshotting;
using Elders.Cronus.Projections.Versioning;

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
            settings.UseSnapshotStrategy(new TimeOffsetSnapshotStrategy(snapshotOffset: TimeSpan.FromDays(1), eventsInSnapshot: 500));

            configure?.Invoke(settings);

            var projectionTypes = (settings as ICassandraProjectionsStoreSettings).ProjectionTypes;

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
            settings.UseSnapshotStrategy(new EventsCountSnapshotStrategy(eventsInSnapshot: 500));

            (settings as ICassandraProjectionsStoreSettings).ProjectionTypes = self.HandlerRegistrations;

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
        public static T SetProjectionsConnectionString<T>(this T self, string connectionString) where T : ICassandraProjectionsStoreSettings
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
        public static T SetProjectionsKeyspace<T>(this T self, string keyspace) where T : ICassandraProjectionsStoreSettings
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
        public static T SetProjectionsCluster<T>(this T self, DataStaxCassandra.Cluster cluster) where T : ICassandraProjectionsStoreSettings
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
        public static T SetProjectionsWriteConsistencyLevel<T>(this T self, DataStaxCassandra.ConsistencyLevel writeConsistencyLevel) where T : ICassandraProjectionsStoreSettings
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
        public static T SetProjectionsReadConsistencyLevel<T>(this T self, DataStaxCassandra.ConsistencyLevel readConsistencyLevel) where T : ICassandraProjectionsStoreSettings
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
        public static T SetProjectionsReconnectionPolicy<T>(this T self, DataStaxCassandra.IReconnectionPolicy policy) where T : ICassandraProjectionsStoreSettings
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
        public static T SetProjectionsRetryPolicy<T>(this T self, DataStaxCassandra.IRetryPolicy policy) where T : ICassandraProjectionsStoreSettings
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
        public static T SetProjectionsReplicationStrategy<T>(this T self, ICassandraReplicationStrategy replicationStrategy) where T : ICassandraProjectionsStoreSettings
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
        public static T SetProjectionTypes<T>(this T self, Assembly projectionsAssembley) where T : ICassandraProjectionsStoreSettings
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
        public static T SetProjectionTypes<T>(this T self, IEnumerable<Type> projectionTypes) where T : ICassandraProjectionsStoreSettings
        {
            self.ProjectionTypes = projectionTypes;
            return self;
        }

        /// <summary>
        /// Set snapshot strategy
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="snapshotStrategy"></param>
        /// <returns></returns>
        public static T UseSnapshotStrategy<T>(this T self, ISnapshotStrategy snapshotStrategy) where T : ICassandraProjectionsStoreSettings
        {
            self.SnapshotStrategy = snapshotStrategy;
            return self;
        }
    }

    public interface ICassandraProjectionsStoreSettings : ISettingsBuilder
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
        ISnapshotStrategy SnapshotStrategy { get; set; }
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
            ICassandraProjectionsStoreSettings settings = this as ICassandraProjectionsStoreSettings;
            base.Build();
            subscrptionMiddlewareSettings.Middleware(x => { return new EventSourcedProjectionsMiddleware(builder.Container.Resolve<IProjectionRepository>(builder.Name)); });
        }
    }

    public class CassandraProjectionsStoreSettings : SettingsBuilder, ICassandraProjectionsStoreSettings
    {
        public CassandraProjectionsStoreSettings(ISettingsBuilder settingsBuilder) : base(settingsBuilder) { }

        public override void Build()
        {
            var builder = this as ISettingsBuilder;
            ICassandraProjectionsStoreSettings settings = this as ICassandraProjectionsStoreSettings;

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


            DataStaxCassandra.ISession schemaSession = GetLiveSchemaSession(cluster, settings);
            schemaSession.CreateKeyspace(settings.ReplicationStrategy, settings.Keyspace);

            var serializer = builder.Container.Resolve<ISerializer>();
            var publisher = builder.Container.Resolve<ITransport>(builder.Name).GetPublisher<ICommand>(serializer);

            var session = cluster.Connect(settings.Keyspace);
            var projectionStoreSchema = new CassandraProjectionStoreSchema(schemaSession);

            builder.Container.RegisterSingleton<IProjectionStore>(() => new CassandraProjectionStore(settings.ProjectionTypes, session, serializer, publisher, projectionStoreSchema), builder.Name);
            if (ReferenceEquals(null, settings.ProjectionTypes))
            {
                builder.Container.RegisterSingleton<ISnapshotStore>(() => new NoSnapshotStore(), builder.Name);
            }
            else
            {
                var snapshotStoreSchema = new CassandraSnapshotStoreSchema(schemaSession);
                builder.Container.RegisterSingleton<ISnapshotStore>(() => new CassandraSnapshotStore(settings.ProjectionTypes, session, serializer, snapshotStoreSchema), builder.Name);
            }

            builder.Container.RegisterSingleton<ISnapshotStrategy>(() => settings.SnapshotStrategy, builder.Name);
        }

        private DataStaxCassandra.ISession GetLiveSchemaSession(DataStaxCassandra.Cluster cluster, ICassandraProjectionsStoreSettings settings)
        {
            var hosts = cluster.AllHosts().ToList();
            DataStaxCassandra.ISession schemaSession = null;
            var counter = 0;

            while (ReferenceEquals(null, schemaSession))
            {
                try
                {
                    var schemaCreatorVoltron = hosts.ElementAtOrDefault(counter++);
                    if (ReferenceEquals(null, schemaCreatorVoltron))
                        throw new InvalidOperationException($"Could not find a Cassandra node! Hosts: '{string.Join(", ", hosts.Select(x => x.Address))}'");

                    var schemaCluster = DataStaxCassandra.Cluster
                           .Builder()
                           .WithReconnectionPolicy(settings.ReconnectionPolicy)
                           .WithRetryPolicy(settings.RetryPolicy)
                           .AddContactPoint(schemaCreatorVoltron.Address)
                           .Build();

                    schemaSession = schemaCluster.Connect();
                }
                catch (DataStaxCassandra.NoHostAvailableException)
                {
                    if (counter < hosts.Count)
                        continue;
                    else
                        throw;
                }
            }

            return schemaSession;
        }

        string ICassandraProjectionsStoreSettings.Keyspace { get; set; }

        string ICassandraProjectionsStoreSettings.ConnectionString { get; set; }

        IEnumerable<Type> ICassandraProjectionsStoreSettings.ProjectionTypes { get; set; }

        DataStaxCassandra.Cluster ICassandraProjectionsStoreSettings.Cluster { get; set; }

        DataStaxCassandra.ConsistencyLevel ICassandraProjectionsStoreSettings.WriteConsistencyLevel { get; set; }

        DataStaxCassandra.ConsistencyLevel ICassandraProjectionsStoreSettings.ReadConsistencyLevel { get; set; }

        DataStaxCassandra.IRetryPolicy ICassandraProjectionsStoreSettings.RetryPolicy { get; set; }

        DataStaxCassandra.IReconnectionPolicy ICassandraProjectionsStoreSettings.ReconnectionPolicy { get; set; }

        ICassandraReplicationStrategy ICassandraProjectionsStoreSettings.ReplicationStrategy { get; set; }

        ISnapshotStrategy ICassandraProjectionsStoreSettings.SnapshotStrategy { get; set; }
    }
}
