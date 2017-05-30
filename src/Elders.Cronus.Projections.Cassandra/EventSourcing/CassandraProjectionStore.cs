using Elders.Cronus.DomainModeling;
using System.Collections.Generic;
using System;
using Cassandra;
using System.Collections.Concurrent;
using Elders.Cronus.Serializer;
using System.IO;
using Elders.Cronus.Projections.Cassandra.Config;
using System.Linq;
using Elders.Cronus.Projections.Cassandra.Snapshots;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class CassandraProjectionStore : IProjectionStore
    {
        public readonly string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, sm int, evarid text, evarrev int, evarts bigint, evarpos int, data blob, PRIMARY KEY ((id, sm), evarid, evarrev, evarpos, evarts)) WITH CLUSTERING ORDER BY (evarid ASC);";
        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, sm, evarid, evarrev, evarpos, evarts, data) VALUES (?,?,?,?,?,?,?);";
        const string GetQueryTemplate = @"SELECT data FROM ""{0}"" WHERE id=? AND sm=?;";
        const string DeleteQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";

        readonly ISession session;
        readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;
        readonly ISerializer serializer;
        readonly IVersionStore versionStore;

        public CassandraProjectionStore(IEnumerable<Type> projections, ISession session, ISerializer serializer, IVersionStore projectionVersionStore)
        {
            if (ReferenceEquals(null, projections) == true) throw new ArgumentNullException(nameof(projections));
            if (ReferenceEquals(null, session) == true) throw new ArgumentNullException(nameof(session));
            if (ReferenceEquals(null, serializer) == true) throw new ArgumentNullException(nameof(serializer));
            if (ReferenceEquals(null, projectionVersionStore) == true) throw new ArgumentNullException(nameof(projectionVersionStore));

            this.serializer = serializer;
            this.session = session;
            this.versionStore = projectionVersionStore;
            this.SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            InitializeProjectionDatabase(projections);
        }

        public ProjectionStream Load<T>(IBlobId projectionId, ISnapshot snapshot) where T : IProjectionDefinition
        {
            return Load(typeof(T), projectionId, snapshot);
        }

        public ProjectionStream Load(Type projectionType, IBlobId projectionId, ISnapshot snapshot)
        {
            string projId = Convert.ToBase64String(projectionId.RawId);
            List<ProjectionCommit> commits = new List<ProjectionCommit>();
            bool tryGetRecords = true;
            int snapshotMarker = snapshot.Revision + 1;
            var version = versionStore.Get(projectionType.GetColumnFamily());

            while (tryGetRecords)
            {
                tryGetRecords = false;
                BoundStatement bs = GetPreparedStatementToGetProjection(version.GetLiveVersionLocation()).Bind(projId, snapshotMarker);
                var result = session.Execute(bs);
                var rows = result.GetRows();
                foreach (var row in rows)
                {
                    tryGetRecords = true;
                    var data = row.GetValue<byte[]>("data");
                    using (var stream = new MemoryStream(data))
                    {
                        commits.Add((ProjectionCommit)serializer.Deserialize(stream));
                    }
                }
                snapshotMarker++;
            }

            return new ProjectionStream(commits, snapshot);
        }

        public void Save(ProjectionCommit commit)
        {
            var builder = new EventSourcedProjectionBuilder(this, commit.ProjectionType, versionStore);
            Save(commit, builder.ProjectionVersion.GetLiveVersionLocation());
            builder.Populate(commit);
        }

        public void Save(ProjectionCommit commit, string columnFamily)
        {
            var data = serializer.SerializeToBytes(commit);
            var statement = SavePreparedStatements.GetOrAdd(columnFamily, x => BuildInsertPreparedStatemnt(x));
            var result = session.Execute(statement
                .Bind(
                    ConvertIdToString(commit.ProjectionId),
                    commit.SnapshotMarker,
                    commit.EventOrigin.AggregateRootId,
                    commit.EventOrigin.AggregateRevision,
                    commit.EventOrigin.AggregateEventPosition,
                    commit.EventOrigin.Timestamp,
                    data
                ));
        }

        public void DropTable(string location)
        {
            session.Execute(string.Format(DeleteQueryTemplate, location));
        }

        public void CreateTable(string template, string location)
        {
            session.Execute(string.Format(template, location));
        }

        private PreparedStatement BuildInsertPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(InsertQueryTemplate, columnFamily));
        }

        private void InitializeProjectionDatabase(IEnumerable<Type> projections)
        {
            foreach (var projType in projections
                .Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x))
                .Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IEventHandler<>))))
            {
                var version = versionStore.Get(projType.GetColumnFamily());
                CreateTable(CreateProjectionEventsTableTemplate, version.GetLiveVersionLocation());
            }
        }

        private PreparedStatement GetPreparedStatementToGetProjection(string columnFamily)
        {
            PreparedStatement loadAggregatePreparedStatement;
            if (!GetPreparedStatements.TryGetValue(columnFamily, out loadAggregatePreparedStatement))
            {
                loadAggregatePreparedStatement = session.Prepare(string.Format(GetQueryTemplate, columnFamily));
                GetPreparedStatements.TryAdd(columnFamily, loadAggregatePreparedStatement);
            }
            return loadAggregatePreparedStatement;
        }

        private string ConvertIdToString(object id)
        {
            if (id is string || id is Guid)
                return id.ToString();

            if (id is IBlobId)
            {
                return Convert.ToBase64String((id as IBlobId).RawId);
            }
            throw new NotImplementedException(String.Format("Unknow type id {0}", id.GetType()));
        }

        public IProjectionBuilder GetBuilder(Type projectionType)
        {
            return new EventSourcedProjectionBuilder(this, projectionType, versionStore);
        }
    }


    public class EventSourcedProjectionBuilder : IProjectionBuilder
    {

        public VersionModel ProjectionVersion { get; private set; }
        public VersionModel SnapshotVersion { get; private set; }
        readonly Type projectionType;
        readonly CassandraProjectionStore store;
        readonly IVersionStore versionStore;

        public EventSourcedProjectionBuilder(CassandraProjectionStore store, Type projectionType, IVersionStore versionStore)
        {
            this.versionStore = versionStore;
            this.store = store;
            this.projectionType = projectionType;

            RefreshVersions();
        }

        void RefreshVersions()
        {
            this.SnapshotVersion = versionStore.Get(projectionType.GetColumnFamily("_sp"));
            this.ProjectionVersion = versionStore.Get(projectionType.GetColumnFamily());
        }

        public void Begin()
        {
            RefreshVersions();
            if (ProjectionVersion.Status != VersionStatus.Live) return;

            // Drop previous projection version
            store.DropTable(ProjectionVersion.GetPreviousVersionLocation());

            // Drop previous snapshot version
            store.DropTable(SnapshotVersion.GetPreviousVersionLocation());

            // Next projection version
            var projectionNextVersion = ProjectionVersion.Version + 1;
            var projectionVersion = new VersionModel(ProjectionVersion.Key, projectionNextVersion, VersionStatus.Building);
            versionStore.Save(projectionVersion);
            ProjectionVersion = projectionVersion;

            // Create next projection table
            var projectionTableToCreate = $"{ProjectionVersion.Key}_{projectionNextVersion}";
            store.CreateTable(store.CreateProjectionEventsTableTemplate, projectionTableToCreate);

            // Next snapshot version
            var snapshotNextVersion = SnapshotVersion.Version + 1;
            var snapshotVersion = new VersionModel(SnapshotVersion.Key, snapshotNextVersion, VersionStatus.Building);
            versionStore.Save(snapshotVersion);
            SnapshotVersion = snapshotVersion;

            // Create next snapshot table
            var snapshotTableToCreate = $"{SnapshotVersion.Key}_{snapshotNextVersion}";
            // We need to fix this
            store.CreateTable(TableTemplates.CreateSnapshopEventsTableTemplate, snapshotTableToCreate);
        }

        public void End()
        {
            RefreshVersions();
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
