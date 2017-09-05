using System;
using Elders.Cronus.Projections.Cassandra.Config;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class EventSourcedProjectionBuilder : IProjectionBuilder
    {
        public VersionModel ProjectionVersion { get; private set; }
        public VersionModel SnapshotVersion { get; private set; }
        readonly string projectionContractId;
        readonly CassandraProjectionStore store;
        readonly IVersionStore versionStore;

        public EventSourcedProjectionBuilder(CassandraProjectionStore store, string projectionContractId, IVersionStore versionStore)
        {
            this.versionStore = versionStore;
            this.store = store;
            this.projectionContractId = projectionContractId;

            RefreshVersions();
        }

        void RefreshVersions()
        {
            this.SnapshotVersion = versionStore.Get(projectionContractId.GetColumnFamily("_sp"));
            this.ProjectionVersion = versionStore.Get(projectionContractId.GetColumnFamily());
        }

        public void Begin()
        {
            if (ProjectionVersion.Status != VersionStatus.Live) return;

            // Prepare next projection version
            var projectionNextVersion = ProjectionVersion.Version + 1;
            var projectionVersion = new VersionModel(ProjectionVersion.Key, projectionNextVersion, VersionStatus.Building);
            ProjectionVersion = projectionVersion;

            // Prepare next snapshot version
            var snapshotNextVersion = SnapshotVersion.Version + 1;
            var snapshotVersion = new VersionModel(SnapshotVersion.Key, snapshotNextVersion, VersionStatus.Building);
            SnapshotVersion = snapshotVersion;

            // Create next snapshot table
            // We need to fix this
            var snapshotTableToCreate = $"{SnapshotVersion.Key}_{snapshotNextVersion}";
            store.CreateTable(TableTemplates.CreateSnapshopEventsTableTemplate, snapshotTableToCreate);

            // Create next projection table
            var projectionTableToCreate = $"{ProjectionVersion.Key}_{projectionNextVersion}";
            store.CreateTable(store.CreateProjectionEventsTableTemplate, projectionTableToCreate);

            // Save next projection version
            versionStore.Save(projectionVersion);

            // Save next snapshot version
            versionStore.Save(snapshotVersion);
        }

        public void End()
        {
            if (ProjectionVersion.Status != VersionStatus.Building) return;

            // Promote next live version of projections
            var projectionVersion = new VersionModel(ProjectionVersion.Key, ProjectionVersion.Version, VersionStatus.Live);
            versionStore.Save(projectionVersion);
            ProjectionVersion = projectionVersion;

            //Promote next live version of snapshots
            var snapshotVersion = new VersionModel(SnapshotVersion.Key, SnapshotVersion.Version, VersionStatus.Live);
            versionStore.Save(snapshotVersion);
            SnapshotVersion = snapshotVersion;

            // Drop obsolate projection version
            var projectionTableToDelete = ProjectionVersion.GetPreviousVersionLocation();
            store.DropTable(projectionTableToDelete);

            // Drop obsolate snapshot version
            var snapshotprojectionTableToDelete = SnapshotVersion.GetPreviousVersionLocation();
            store.DropTable(snapshotprojectionTableToDelete);
        }

        public void Populate(ProjectionCommit commit)
        {
            if (ProjectionVersion.Status == VersionStatus.Building)
            {
                store.Save(commit, ProjectionVersion.GetNextVersionLocation());
            }
        }
    }
}
