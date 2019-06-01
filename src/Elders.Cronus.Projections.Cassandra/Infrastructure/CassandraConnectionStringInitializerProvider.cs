using System;
using Cassandra;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public class CassandraConnectionStringInitializerProvider : ICassandraInitializerProvider
    {
        Builder builder;

        protected CassandraProviderOptions options;

        public CassandraConnectionStringInitializerProvider(IOptionsMonitor<CassandraProviderOptions> optionsMonitor)
        {
            if (optionsMonitor is null) throw new ArgumentNullException(nameof(optionsMonitor));
            options = optionsMonitor.CurrentValue;
            optionsMonitor.OnChange(Changed);
        }

        public bool ConfigurationHasChanged { get; protected set; }

        private void Changed(CassandraProviderOptions newOptions)
        {
            if (options != newOptions)
            {
                options = newOptions;
                ConfigurationHasChanged = true;
            }
        }

        protected virtual Builder Configure(Builder builder)
        {
            return builder;
        }

        private Builder ConfigureInternally(Builder builder)
        {
            return builder
                .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                .WithRetryPolicy(new NoHintedHandOffRetryPolicy());
        }

        public IInitializer GetInitializer()
        {
            if (ConfigurationHasChanged || builder is null)
            {
                var connStrBuilder = new CassandraConnectionStringBuilder(options.ConnectionString);

                builder = connStrBuilder.ApplyToBuilder(builder);
                builder = ConfigureInternally(builder);
            }

            ConfigurationHasChanged = false;

            return builder;
        }
    }
}
