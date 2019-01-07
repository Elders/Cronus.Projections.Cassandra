using Elders.Cronus.Discoveries;
using Microsoft.Extensions.Configuration;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProviderOptionsProvider : CronusOptionsProviderBase<CassandraProviderOptions>
    {
        public CassandraProviderOptionsProvider(IConfiguration configuration) : base(configuration) { }

        public override void Configure(CassandraProviderOptions options)
        {
            options.ConnectionString = configuration["cronus_projections_cassandra_connectionstring"];
        }
    }
}
