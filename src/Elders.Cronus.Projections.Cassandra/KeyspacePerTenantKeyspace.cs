using System;
using Elders.Cronus.MessageProcessing;

namespace Elders.Cronus.Projections.Cassandra
{
    public sealed class KeyspacePerTenantKeyspace : IKeyspaceNamingStrategy
    {
        private readonly ICronusContextAccessor contextAccessor;

        public KeyspacePerTenantKeyspace(ICronusContextAccessor contextAccessor)
        {
            this.contextAccessor = contextAccessor;
        }

        public string GetName(string baseConfigurationKeyspace)
        {
            string tenantPrefix = string.IsNullOrEmpty(contextAccessor.CronusContext.Tenant) ? string.Empty : $"{contextAccessor.CronusContext.Tenant}_";
            var keyspace = $"{tenantPrefix}{baseConfigurationKeyspace}";
            if (keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {keyspace}");

            return keyspace;
        }
    }
}
