using Microsoft.Extensions.Configuration;

namespace Elders.Cronus.Projections.Cassandra.Infrastructure;

public class TableRetentionOptionsProvider : CronusOptionsProviderBase<TableRetentionOptions>
{
    public const string SettingKey = "cronus:projections:cassandra:tableretention";

    public TableRetentionOptionsProvider(IConfiguration configuration) : base(configuration) { }

    public override void Configure(TableRetentionOptions options)
    {
        configuration.GetSection(SettingKey).Bind(options);
    }
}
