using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Cassandra;
using Elders.Cronus.Projections.Cassandra.EventSourcing;
using Elders.Cronus.Projections.Snapshotting;
using Elders.Cronus.Serializer;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public sealed class CassandraSnapshotStore : ISnapshotStore
    {
        static readonly object createMutex = new object();
        static readonly object dropMutex = new object();

        const string CreateSnapshopEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, rev int, data blob, PRIMARY KEY (id, rev)) WITH CLUSTERING ORDER BY (rev DESC);";
        const string DropQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";
        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, rev, data) VALUES (?,?,?);";
        const string GetQueryTemplate = @"SELECT data, rev FROM ""{0}"" WHERE id=? LIMIT 1;";
        const string GetSnapshotMetaQueryTemplate = @"SELECT rev FROM ""{0}"" WHERE id=? LIMIT 1;";

        readonly ISession session;
        readonly HashSet<string> projectionContracts;
        readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetSnapshotMetaPreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> CreatePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> DropPreparedStatements;
        readonly ISerializer serializer;

        public CassandraSnapshotStore(IEnumerable<Type> projections, ISession session, ISerializer serializer)
        {
            if (ReferenceEquals(null, projections) == true) throw new ArgumentNullException(nameof(projections));
            this.projectionContracts = new HashSet<string>(
                projections
                .Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x))
                .Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IEventHandler<>)))
                .Select(proj => proj.GetContractId()));

            if (ReferenceEquals(null, session) == true) throw new ArgumentNullException(nameof(session));
            if (ReferenceEquals(null, serializer) == true) throw new ArgumentNullException(nameof(serializer));

            this.serializer = serializer;
            this.session = session;

            this.SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.GetSnapshotMetaPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.CreatePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.DropPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            //InitializeSnapshotsDatabase();
        }

        public ISnapshot Load(string projectionName, IBlobId id, ProjectionVersion version)
        {
            if (projectionContracts.Contains(projectionName) == false)
                return new NoSnapshot(id, projectionName);

            var columnFamily = version.GetSnapshotColumnFamily();

            return Load(projectionName, id, columnFamily);
        }

        ISnapshot Load(string projectionName, IBlobId id, string columnFamily)
        {
            BoundStatement bs = GetPreparedStatementToGetProjection(columnFamily).Bind(Convert.ToBase64String(id.RawId));
            var result = session.Execute(bs);
            var row = result.GetRows().FirstOrDefault();

            if (row == null)
                return new NoSnapshot(id, projectionName);

            var data = row.GetValue<byte[]>("data");
            var rev = row.GetValue<int>("rev");

            using (var stream = new MemoryStream(data))
            {
                return new Snapshot(id, projectionName, serializer.Deserialize(stream), rev);
            }
        }

        public SnapshotMeta LoadMeta(string projectionName, IBlobId id, ProjectionVersion version)
        {
            if (projectionContracts.Contains(projectionName) == false)
                return new NoSnapshot(id, projectionName).GetMeta();

            var columnFamily = version.GetSnapshotColumnFamily();

            return LoadMeta(projectionName, id, columnFamily);
        }

        SnapshotMeta LoadMeta(string projectionName, IBlobId id, string columnFamily)
        {
            BoundStatement bs = GetPreparedStatementToGetSnapshotMeta(columnFamily).Bind(Convert.ToBase64String(id.RawId));
            var result = session.Execute(bs);
            var row = result.GetRows().FirstOrDefault();

            if (row == null)
                return new NoSnapshot(id, projectionName).GetMeta();

            var rev = row.GetValue<int>("rev");

            return new SnapshotMeta(rev, projectionName);
        }

        public void Save(ISnapshot snapshot, ProjectionVersion projectionVersion)
        {
            if (projectionContracts.Contains(snapshot.ProjectionName) == false)
                return;

            var columnFamily = projectionVersion.GetSnapshotColumnFamily();

            Save(snapshot, columnFamily);
        }

        void Save(ISnapshot snapshot, string columnFamily)
        {
            var data = serializer.SerializeToBytes(snapshot.State);
            var statement = SavePreparedStatements.GetOrAdd(columnFamily, x => BuildeInsertPreparedStatemnt(x));
            var result = session.Execute(statement
                .Bind(
                    Convert.ToBase64String(snapshot.Id.RawId),
                    snapshot.Revision,
                    data
                ));
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
                var statement = CreatePreparedStatements.GetOrAdd(location, x => BuildCreatePreparedStatement(template, x));
                statement.SetConsistencyLevel(ConsistencyLevel.All);
                session.Execute(statement.Bind());
            }
        }

        public void InitializeProjectionSnapshotStore(ProjectionVersion projectionVersion)
        {
            CreateTable(CreateSnapshopEventsTableTemplate, projectionVersion.GetSnapshotColumnFamily());
        }

        PreparedStatement BuildDropPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(DropQueryTemplate, columnFamily));
        }

        PreparedStatement BuildCreatePreparedStatement(string template, string columnFamily)
        {
            return session.Prepare(string.Format(template, columnFamily));
        }

        PreparedStatement BuildeInsertPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(InsertQueryTemplate, columnFamily));
        }

        PreparedStatement GetPreparedStatementToGetProjection(string columnFamily)
        {
            return GetPreparedStatements.GetOrAdd(columnFamily, session.Prepare(string.Format(GetQueryTemplate, columnFamily)));
        }

        PreparedStatement GetPreparedStatementToGetSnapshotMeta(string columnFamily)
        {
            return GetSnapshotMetaPreparedStatements.GetOrAdd(columnFamily, session.Prepare(string.Format(GetSnapshotMetaQueryTemplate, columnFamily)));
        }
    }
}
