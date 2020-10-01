using System.Collections.Generic;
using System.Linq;
using Cassandra;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public class RetainOldProjectionRevisions : IProjectionTableRetentionStrategy
    {
        private readonly ICluster cluster;
        private readonly VersionedProjectionsNaming projectionsNaming;
        private readonly CassandraProjectionStoreSchema projectionsSchema;
        private readonly CassandraSnapshotStoreSchema snapshotsSchema;
        private TableRetentionOptions options;

        public RetainOldProjectionRevisions(ICassandraProvider cassandraProvider, VersionedProjectionsNaming projectionsNaming, CassandraProjectionStoreSchema projectionsSchema, CassandraSnapshotStoreSchema snapshotsSchema, IOptionsMonitor<TableRetentionOptions> optionsMonitor)
        {
            this.cluster = cassandraProvider.GetCluster();
            this.projectionsNaming = projectionsNaming;
            this.projectionsSchema = projectionsSchema;
            this.snapshotsSchema = snapshotsSchema;
            options = optionsMonitor.CurrentValue;
            optionsMonitor.OnChange(newOptions => options = newOptions);
        }

        public void Apply(ProjectionVersion currentProjectionVersion)
        {
            if (options.DeleteOldProjectionTables == false)
                return;

            var latestRevisionToRetain = currentProjectionVersion.Revision - options.NumberOfOldProjectionTablesToRetain;
            if (latestRevisionToRetain <= 0)
                return;

            var tables = GetProjectionVersions(projectionsSchema.Keyspace);
            foreach (var table in tables)
            {
                projectionsSchema.DropTable(table);
            }

            IEnumerable<string> GetProjectionVersions(string keyspace)
            {
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
