using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Cassandra;
using Elders.Cronus.Projections.Cassandra.Config;
using Elders.Cronus.Projections.Cassandra.EventSourcing;
using Elders.Cronus.Projections.Cassandra.Logging;
using Elders.Cronus.Projections.Snapshotting;
using Elders.Cronus.Serializer;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public class ProjectionsProvider
    {
        private readonly IEnumerable<Type> projectionTypes;

        public ProjectionsProvider(IEnumerable<Type> projectionTypes)
        {
            this.projectionTypes = projectionTypes;
        }

        public IEnumerable<Type> GetProjections()
        {
            return projectionTypes;
        }
    }

    public sealed class CassandraSnapshotStore : ISnapshotStore
    {
        static ILog log = LogProvider.GetLogger(typeof(CassandraSnapshotStore));

        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, rev, data) VALUES (?,?,?);";
        const string GetQueryTemplate = @"SELECT data, rev FROM ""{0}"" WHERE id=? LIMIT 1;";
        const string GetSnapshotMetaQueryTemplate = @"SELECT rev FROM ""{0}"" WHERE id=? LIMIT 1;";

        readonly ISession session;
        readonly HashSet<string> projectionContracts;
        readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetSnapshotMetaPreparedStatements;
        readonly ISerializer serializer;
        private readonly CassandraSnapshotStoreSchema schema;

        public CassandraSnapshotStore(IEnumerable<Type> projections, ISession session, ISerializer serializer, CassandraSnapshotStoreSchema schema)
        {
            if (ReferenceEquals(null, projections) == true) throw new ArgumentNullException(nameof(projections));
            if (ReferenceEquals(null, session) == true) throw new ArgumentNullException(nameof(session));
            if (ReferenceEquals(null, serializer) == true) throw new ArgumentNullException(nameof(serializer));

            projectionContracts = new HashSet<string>(
                projections
                .Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x))
                .Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IEventHandler<>)))
                .Select(proj => proj.GetContractId()));

            this.session = session;
            this.serializer = serializer;
            this.schema = schema;

            SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetSnapshotMetaPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public CassandraSnapshotStore(ProjectionsProvider projectionsProvider, CassandraProvider cassandraProvider, ISerializer serializer, CassandraSnapshotStoreSchema schema)
            : this(projectionsProvider.GetProjections(), cassandraProvider.GetSession(), serializer, schema)
        {

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
            Row row = null;

            try
            {
                var bs = GetPreparedStatementToGetProjection(columnFamily).Bind(Convert.ToBase64String(id.RawId));
                var result = session.Execute(bs);
                row = result.GetRows().FirstOrDefault();
            }
            catch (InvalidQueryException)
            {
                if (ReferenceEquals(null, schema) == false)
                {
                    schema.CreateTable(columnFamily);
                }
                else
                {
                    log.Debug(() => "This node can not change Cassandra schema.");
                }
            }

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
            Row row = null;

            try
            {
                var bs = GetPreparedStatementToGetSnapshotMeta(columnFamily).Bind(Convert.ToBase64String(id.RawId));
                var result = session.Execute(bs);
                row = result.GetRows().FirstOrDefault();
            }
            catch (InvalidQueryException)
            {
                if (ReferenceEquals(null, schema) == false)
                {
                    schema.CreateTable(columnFamily);
                }
                else
                {
                    log.Debug(() => "This node can not change Cassandra schema.");
                }
            }

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

        public void InitializeProjectionSnapshotStore(ProjectionVersion projectionVersion)
        {
            if (ReferenceEquals(null, schema) == false)
            {
                log.Debug(() => $"Initializing projection snapshot store with column family '{projectionVersion.GetColumnFamily()}' for projection version '{projectionVersion}'");
                schema.CreateTable(projectionVersion.GetColumnFamily());
                return;
            }

            log.Debug(() => "This node can not change Cassandra schema.");
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
