using System.Collections.Generic;
using System.Linq;
using Cassandra;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public class RetainOldProjectionRevisions : IProjectionTableRetentionStrategy
    {
        private readonly ICluster cluster;
        private readonly IProjectionsNamingStrategy projectionsNaming;
        private readonly CassandraProjectionStoreSchema projectionsSchema;
        private readonly CassandraSnapshotStoreSchema snapshotsSchema;
        private TableRetentionOptions options;

        public RetainOldProjectionRevisions(ICluster cluster, IProjectionsNamingStrategy projectionsNaming, CassandraProjectionStoreSchema projectionsSchema, CassandraSnapshotStoreSchema snapshotsSchema, IOptionsMonitor<TableRetentionOptions> optionsMonitor)
        {
            this.cluster = cluster;
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

            var projectionsVersionsToDelete = GetProjectionVersions(projectionsSchema.Keyspace);
            foreach (var version in projectionsVersionsToDelete)
            {
                var projectionTableName = projectionsNaming.GetColumnFamily(version);
                projectionsSchema.DropTable(projectionTableName);
            }

            var projectionsVersionsSnapshotsToDelete = GetProjectionVersions(snapshotsSchema.Keyspace);
            foreach (var version in projectionsVersionsSnapshotsToDelete)
            {
                var snapshotTableName = projectionsNaming.GetSnapshotColumnFamily(version);
                snapshotsSchema.DropTable(snapshotTableName);
            }

            IEnumerable<ProjectionVersion> GetProjectionVersions(string keyspace)
            {
                var projectionsTables = cluster.Metadata.GetTables(keyspace);
                var projectionsVersionsToDelete = projectionsTables
                    .Select(x => projectionsNaming.Parse(x))
                    .Where(x => x.ProjectionName == currentProjectionVersion.ProjectionName && x.Revision < latestRevisionToRetain); // handles cases when the hash might change

                return projectionsVersionsToDelete;
            }
        }
    }
}
