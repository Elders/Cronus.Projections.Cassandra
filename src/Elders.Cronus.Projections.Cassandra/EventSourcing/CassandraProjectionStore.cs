using Elders.Cronus.DomainModeling;
using Elders.Cronus.DomainModeling.Projections;
using System.Collections.Generic;
using System;
using Cassandra;
using System.Collections.Concurrent;
using Elders.Cronus.Serializer;
using System.IO;
using Elders.Cronus.Projections.Cassandra.Config;
using System.Linq;
using Elders.Cronus.Projections.Cassandra.Snapshots;
using Elders.Cronus.Projections.Cassandra.Logging;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class CassandraProjectionStore : IProjectionStore
    {
        static ILog log = LogProvider.GetLogger(typeof(CassandraProjectionStore));

        static readonly object createMutex = new object();
        static readonly object dropMutex = new object();

        public readonly string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, sm int, evarid text, evarrev int, evarts bigint, evarpos int, data blob, PRIMARY KEY ((id, sm), evarid, evarrev, evarpos, evarts)) WITH CLUSTERING ORDER BY (evarid ASC);";
        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, sm, evarid, evarrev, evarpos, evarts, data) VALUES (?,?,?,?,?,?,?);";
        const string GetQueryTemplate = @"SELECT data FROM ""{0}"" WHERE id=? AND sm=?;";
        const string DropQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";

        readonly ISession session;
        readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> CreatePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> DropPreparedStatements;
        readonly ISerializer serializer;
        readonly IVersionStore versionStore;

        public VersionModel SnapshotVersion { get; private set; }

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
            this.CreatePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.DropPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            InitializeProjectionDatabase(projections);
        }

        public ProjectionStream Load(string contractId, IBlobId projectionId, ISnapshot snapshot, bool isReplay)
        {
            var version = versionStore.Get(contractId.GetColumnFamily());

            // Live
            if (isReplay == false)
                return Load(contractId, projectionId, snapshot, version.GetLiveVersionLocation());

            // Replay
            return Load(contractId, projectionId, snapshot, version.GetNextVersionLocation());
        }

        private ProjectionStream Load(string contractId, IBlobId projectionId, ISnapshot snapshot, string columnFamily)
        {
            string projId = Convert.ToBase64String(projectionId.RawId);
            List<ProjectionCommit> commits = new List<ProjectionCommit>();
            bool tryGetRecords = true;
            int snapshotMarker = snapshot.Revision + 1;

            while (tryGetRecords)
            {
                tryGetRecords = false;
                BoundStatement bs = GetPreparedStatementToGetProjection(columnFamily).Bind(projId, snapshotMarker);
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

            if (commits.Count > 1000)
                log.Warn($"Potential memory leak. The system will be down fearly soon. The projection `{contractId}` for id={projectionId} loads a lot of projection commits ({commits.Count}) and snapshot `{snapshot.GetType().Name}` which puts a lot of CPU and RAM pressure. You can resolve this by enabling the Snapshots feature in the host which handles projection WRITES and READS using `.UseSnapshots(...)`.");

            return new ProjectionStream(commits, snapshot);
        }

        public void Save(ProjectionCommit commit, bool isReplay)
        {
            var projectionVersion = versionStore.Get(commit.ContractId.GetColumnFamily());

            if (isReplay == false)
                Save(commit, projectionVersion.GetLiveVersionLocation());

            if (projectionVersion.Status == VersionStatus.Building)
                Save(commit, projectionVersion.GetNextVersionLocation());
        }

        private void Save(ProjectionCommit commit, string columnFamily)
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

        public void BeginReplay(string projectionContractId)
        {
            var projectionVersion = versionStore.Get(projectionContractId.GetColumnFamily());
            var snapshotVersion = versionStore.Get(projectionContractId.GetColumnFamily("_sp"));

            if (projectionVersion.Status == VersionStatus.Building || snapshotVersion.Status == VersionStatus.Building)
                return;

            // Prepare next projection version
            var projectionNextVersion = new VersionModel(projectionVersion.Key, projectionVersion.Version + 1, VersionStatus.Building);
            projectionVersion = projectionNextVersion;

            // Prepare next snapshot version
            var snapshotNextVersion = new VersionModel(snapshotVersion.Key, snapshotVersion.Version + 1, VersionStatus.Building);
            snapshotVersion = snapshotNextVersion;

            // Create next snapshot table
            // We need to fix this
            var snapshotTableToCreate = $"{snapshotVersion.Key}_{snapshotVersion.Version}";
            this.CreateTable(TableTemplates.CreateSnapshopEventsTableTemplate, snapshotTableToCreate);

            // Create next projection table
            var projectionTableToCreate = $"{projectionVersion.Key}_{projectionVersion.Version}";
            this.CreateTable(this.CreateProjectionEventsTableTemplate, projectionTableToCreate);

            // Save next projection version
            versionStore.Save(projectionNextVersion);

            // Save next snapshot version
            versionStore.Save(snapshotNextVersion);
        }

        public void EndReplay(string projectionContractId)
        {
            var projectionVersion = versionStore.Get(projectionContractId.GetColumnFamily());
            var snapshotVersion = versionStore.Get(projectionContractId.GetColumnFamily("_sp"));

            if (projectionVersion.Status != VersionStatus.Building || snapshotVersion.Status != VersionStatus.Building)
                return;

            // Promote next live version of projections
            var projectionNextVersion = new VersionModel(projectionVersion.Key, projectionVersion.Version, VersionStatus.Live);
            versionStore.Save(projectionNextVersion);
            projectionVersion = projectionNextVersion;

            // Promote next live version of snapshots
            var snapshotNextVersion = new VersionModel(snapshotVersion.Key, snapshotVersion.Version, VersionStatus.Live);
            versionStore.Save(snapshotNextVersion);
            snapshotVersion = snapshotNextVersion;

            // Drop obsolate projection version
            var projectionTableToDelete = projectionVersion.GetPreviousVersionLocation();
            this.DropTable(projectionTableToDelete);

            // Drop obsolate snapshot version
            var snapshotprojectionTableToDelete = snapshotVersion.GetPreviousVersionLocation();
            this.DropTable(snapshotprojectionTableToDelete);
        }

        public void DropTable(string location)
        {
            // https://issues.apache.org/jira/browse/CASSANDRA-10699
            // https://issues.apache.org/jira/browse/CASSANDRA-11429
            lock (dropMutex)
            {
                var statement = CreatePreparedStatements.GetOrAdd(location, x => BuildDropPreparedStatemnt(x));
                statement.SetConsistencyLevel(ConsistencyLevel.All);
                session.Execute(statement.Bind());
            }
        }

        public void CreateTable(string template, string location)
        {
            // https://issues.apache.org/jira/browse/CASSANDRA-10699
            // https://issues.apache.org/jira/browse/CASSANDRA-11429
            lock (createMutex)
            {
                var statement = CreatePreparedStatements.GetOrAdd(location, x => BuildCreatePreparedStatemnt(template, x));
                statement.SetConsistencyLevel(ConsistencyLevel.All);
                session.Execute(statement.Bind());
            }
        }

        private PreparedStatement BuildInsertPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(InsertQueryTemplate, columnFamily));
        }

        private PreparedStatement BuildDropPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(DropQueryTemplate, columnFamily));
        }

        private PreparedStatement BuildCreatePreparedStatemnt(string template, string columnFamily)
        {
            return session.Prepare(string.Format(template, columnFamily));
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
    }
}
