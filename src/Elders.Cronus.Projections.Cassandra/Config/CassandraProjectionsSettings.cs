using System;
using Elders.Cronus.IocContainer;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Projections.Cassandra.ReplicationStrategies;
using DataStaxCassandra = Cassandra;
using System.Collections.Generic;
using Elders.Cronus.DomainModeling.Projections;

namespace Elders.Cronus.Projections.Cassandra.Config
{
    public static class CassandraEventStoreExtensions
    {
        public static T UseCassandraProjections<T>(this T self, Action<CassandraProjectionsSettings> configure) where T : ProjectionMessageProcessorSettings
        {
            CassandraProjectionsSettings settings = new CassandraProjectionsSettings(self);
            settings.SetReconnectionPolicy(new DataStaxCassandra.ExponentialReconnectionPolicy(100, 100000));
            settings.SetRetryPolicy(new NoHintedHandOffRetryPolicy());
            settings.SetReplicationStrategy(new SimpleReplicationStrategy(1));
            (settings as ICassandraProjectionsSettings).ProjectionTypes = (self as ISubscrptionMiddlewareSettings).HandlerRegistrations;
            configure?.Invoke(settings);

            (settings as ISettingsBuilder).Build();
            return self;
        }

        /// <summary>
        /// Set the connection string.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="connectionString">Connection string that will be used to connect to the cassandra cluster.</param>
        /// <returns></returns>
        public static T SetConnectionString<T>(this T self, string connectionString) where T : ICassandraProjectionsSettings
        {
            var builder = new DataStaxCassandra.CassandraConnectionStringBuilder(connectionString);
            if (string.IsNullOrWhiteSpace(builder.DefaultKeyspace) == false)
            {
                self.ConnectionString = connectionString.Replace(builder.DefaultKeyspace, "");
                self.SetKeyspace(builder.DefaultKeyspace);
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
        public static T SetKeyspace<T>(this T self, string keyspace) where T : ICassandraProjectionsSettings
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
        public static T SetCluster<T>(this T self, DataStaxCassandra.Cluster cluster) where T : ICassandraProjectionsSettings
        {
            self.Cluster = cluster;
            return self;
        }

        /// <summary>
        /// Use to override the default reconnection policy.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="self"></param>
        /// <param name="policy">Cassandra reconnection policy.</param>
        /// <returns></returns>
        public static T SetReconnectionPolicy<T>(this T self, DataStaxCassandra.IReconnectionPolicy policy) where T : ICassandraProjectionsSettings
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
        public static T SetRetryPolicy<T>(this T self, DataStaxCassandra.IRetryPolicy policy) where T : ICassandraProjectionsSettings
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
        public static T SetReplicationStrategy<T>(this T self, ICassandraReplicationStrategy replicationStrategy) where T : ICassandraProjectionsSettings
        {
            self.ReplicationStrategy = replicationStrategy;
            return self;
        }
    }

    public interface ICassandraProjectionsSettings : ISettingsBuilder
    {
        string Keyspace { get; set; }
        string ConnectionString { get; set; }
        IEnumerable<Type> ProjectionTypes { get; set; }
        DataStaxCassandra.Cluster Cluster { get; set; }
        DataStaxCassandra.IRetryPolicy RetryPolicy { get; set; }
        DataStaxCassandra.IReconnectionPolicy ReconnectionPolicy { get; set; }
        ICassandraReplicationStrategy ReplicationStrategy { get; set; }
    }

    public class CassandraProjectionsSettings : SettingsBuilder, ICassandraProjectionsSettings
    {
        public CassandraProjectionsSettings(ISettingsBuilder settingsBuilder) : base(settingsBuilder) { }

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
            var storageManager = new CassandraProjectionsStorageManager(session, settings.Keyspace, settings.ReplicationStrategy, settings.ProjectionTypes);
            storageManager.CreateStorage();
            session.ChangeKeyspace(settings.Keyspace);

            var persister = new CassandraPersister(session);

            builder.Container.RegisterSingleton<IPersiter>(() => persister, builder.Name);
        }

        string ICassandraProjectionsSettings.Keyspace { get; set; }

        string ICassandraProjectionsSettings.ConnectionString { get; set; }

        IEnumerable<Type> ICassandraProjectionsSettings.ProjectionTypes { get; set; }

        DataStaxCassandra.Cluster ICassandraProjectionsSettings.Cluster { get; set; }

        DataStaxCassandra.IRetryPolicy ICassandraProjectionsSettings.RetryPolicy { get; set; }

        DataStaxCassandra.IReconnectionPolicy ICassandraProjectionsSettings.ReconnectionPolicy { get; set; }

        ICassandraReplicationStrategy ICassandraProjectionsSettings.ReplicationStrategy { get; set; }
    }
}
