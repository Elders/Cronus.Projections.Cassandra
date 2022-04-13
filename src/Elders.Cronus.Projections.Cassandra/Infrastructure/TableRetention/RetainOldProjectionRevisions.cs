using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public class RetainOldProjectionRevisions : IProjectionTableRetentionStrategy
    {
        private readonly ICassandraProvider cassandraProvider;
        private readonly VersionedProjectionsNaming projectionsNaming;
        private readonly CassandraProjectionStoreSchema projectionsSchema;
        private readonly CassandraSnapshotStoreSchema snapshotsSchema;
        private TableRetentionOptions options;

        public RetainOldProjectionRevisions(ICassandraProvider cassandraProvider, VersionedProjectionsNaming projectionsNaming, CassandraProjectionStoreSchema projectionsSchema, CassandraSnapshotStoreSchema snapshotsSchema, IOptionsMonitor<TableRetentionOptions> optionsMonitor)
        {
            this.cassandraProvider = cassandraProvider;
            this.projectionsNaming = projectionsNaming;
            this.projectionsSchema = projectionsSchema;
            this.snapshotsSchema = snapshotsSchema;
            options = optionsMonitor.CurrentValue;
            optionsMonitor.OnChange(newOptions => options = newOptions);
        }

        public async Task ApplyAsync(ProjectionVersion currentProjectionVersion)
        {
            if (options.DeleteOldProjectionTables == false)
                return;

            var latestRevisionToRetain = currentProjectionVersion.Revision - options.NumberOfOldProjectionTablesToRetain;
            if (latestRevisionToRetain <= 0)
                return;

            string keyspace = await projectionsSchema.GetKeypaceAsync().ConfigureAwait(false);
            IAsyncEnumerable<string> tables = GetProjectionVersionsAsync(keyspace);
            await foreach (var table in tables)
            {
                await projectionsSchema.DropTableAsync(table).ConfigureAwait(false);
            }

            async IAsyncEnumerable<string> GetProjectionVersionsAsync(string keyspace)
            {
                ICluster cluster = await cassandraProvider.GetClusterAsync().ConfigureAwait(false);
                ICollection<string> projectionsTables = cluster.Metadata.GetTables(keyspace);
                var projectionTablesWithoutHash = GetPossibleTableNamesWihtoutHash();

                foreach (var table in projectionsTables)
                {
                    if (projectionTablesWithoutHash.Any(x => table.StartsWith(x, System.StringComparison.OrdinalIgnoreCase)))
                        yield return table;
                }
            }

            List<string> GetPossibleTableNamesWihtoutHash()
            {
                List<string> result = new List<string>();

                for (long i = latestRevisionToRetain - 1; i > 0; i--)
                {
                    string projectionTableWithoutHash = $"{projectionsNaming.NormalizeProjectionName(currentProjectionVersion.ProjectionName)}_{i}_";
                    result.Add(projectionTableWithoutHash);
                }

                return result;
            }
        }
    }
}
