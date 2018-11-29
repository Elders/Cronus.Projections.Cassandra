using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Cassandra;
using Elders.Cronus.Projections.Cassandra.Logging;
using Elders.Cronus.Projections.Snapshotting;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraSnapshotStore<TSettings> : CassandraSnapshotStore where TSettings : ICassandraProjectionStoreSettings
    {
        public CassandraSnapshotStore(TSettings settings) : base(settings.CassandraProvider, settings.Serializer, settings.ProjectionsNamingStrategy, settings.ProjectionsProvider) { }
    }

    public class CassandraSnapshotStore : ISnapshotStore
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
        private readonly IProjectionsNamingStrategy naming;

        public CassandraSnapshotStore(ICassandraProvider cassandraProvider, ISerializer serializer, IProjectionsNamingStrategy naming, ProjectionsProvider projectionsProvider)
        {
            if (projectionsProvider is null) throw new ArgumentNullException(nameof(projectionsProvider));
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            if (serializer is null) throw new ArgumentNullException(nameof(serializer));
            if (naming is null) throw new ArgumentNullException(nameof(naming));

            projectionContracts = new HashSet<string>(
                projectionsProvider.GetProjections()
                .Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x))
                .Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IEventHandler<>)))
                .Select(proj => proj.GetContractId()));

            this.session = cassandraProvider.GetSession();
            this.serializer = serializer;
            this.naming = naming;

            SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetSnapshotMetaPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public ISnapshot Load(string projectionName, IBlobId id, ProjectionVersion version)
        {
            if (projectionContracts.Contains(projectionName) == false)
                return new NoSnapshot(id, projectionName);

            string columnFamily = naming.GetSnapshotColumnFamily(version);

            return Load(projectionName, id, columnFamily);
        }

        ISnapshot Load(string projectionName, IBlobId id, string columnFamily)
        {
            Row row = null;
            var bs = GetPreparedStatementToGetProjection(columnFamily).Bind(Convert.ToBase64String(id.RawId));
            var result = session.Execute(bs);
            row = result.GetRows().FirstOrDefault();

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

            string columnFamily = naming.GetSnapshotColumnFamily(version);

            return LoadMeta(projectionName, id, columnFamily);
        }

        SnapshotMeta LoadMeta(string projectionName, IBlobId id, string columnFamily)
        {
            Row row = null;

            var bs = GetPreparedStatementToGetSnapshotMeta(columnFamily).Bind(Convert.ToBase64String(id.RawId));
            var result = session.Execute(bs);
            row = result.GetRows().FirstOrDefault();

            if (row == null)
                return new NoSnapshot(id, projectionName).GetMeta();

            var rev = row.GetValue<int>("rev");

            return new SnapshotMeta(rev, projectionName);
        }

        public void Save(ISnapshot snapshot, ProjectionVersion version)
        {
            if (projectionContracts.Contains(snapshot.ProjectionName) == false)
                return;

            string columnFamily = naming.GetSnapshotColumnFamily(version);

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
