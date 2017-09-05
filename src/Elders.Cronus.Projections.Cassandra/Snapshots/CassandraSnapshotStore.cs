using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Cassandra;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.DomainModeling.Projections;
using Elders.Cronus.Projections.Cassandra.Config;
using Elders.Cronus.Projections.Cassandra.EventSourcing;
using Elders.Cronus.Serializer;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public class CassandraSnapshotStore : ISnapshotStore
    {
        //public readonly string CreateSnapshopEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, rev int, data blob, PRIMARY KEY (id, rev)) WITH CLUSTERING ORDER BY (rev DESC);";
        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, rev, data) VALUES (?,?,?);";
        const string GetQueryTemplate = @"SELECT data, rev FROM ""{0}"" WHERE id=? LIMIT 1;";

        public CassandraSnapshotStore(IEnumerable<Type> projections, ISession session, ISerializer serializer, IVersionStore versionStore)
        {
            if (ReferenceEquals(null, projections) == true) throw new ArgumentNullException(nameof(projections));
            this.projectionContracts = new HashSet<string>(
                projections
                .Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x))
                .Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IEventHandler<>)))
                .Select(proj => proj.GetContractId()));
            if (ReferenceEquals(null, session) == true) throw new ArgumentNullException(nameof(session));
            if (ReferenceEquals(null, serializer) == true) throw new ArgumentNullException(nameof(serializer));
            if (ReferenceEquals(null, versionStore) == true) throw new ArgumentNullException(nameof(versionStore));

            this.serializer = serializer;
            this.session = session;
            this.versionStore = versionStore;

            this.SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            InitializeSnapshotsDatabase();
        }

        readonly ISession session;
        readonly HashSet<string> projectionContracts;
        readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;
        readonly ISerializer serializer;
        readonly IVersionStore versionStore;

        public ISnapshot Load(string projectionContractId, IBlobId id)
        {
            if (projectionContracts.Contains(projectionContractId) == false)
                return new NoSnapshot(id, projectionContractId);

            var version = versionStore.Get(projectionContractId.GetColumnFamily("_sp"));

            return Load(projectionContractId, id, version.GetLiveVersionLocation());
        }

        private ISnapshot Load(string projectionContractId, IBlobId id, string columnFamily)
        {
            BoundStatement bs = GetPreparedStatementToGetProjection(columnFamily).Bind(Convert.ToBase64String(id.RawId));
            var result = session.Execute(bs);
            var row = result.GetRows().FirstOrDefault();

            if (row == null)
                return new NoSnapshot(id, projectionContractId);

            var data = row.GetValue<byte[]>("data");
            var rev = row.GetValue<int>("rev");

            using (var stream = new MemoryStream(data))
            {
                return new Snapshot(id, projectionContractId, serializer.Deserialize(stream), rev);
            }
        }

        public void Save(ISnapshot snapshot)
        {
            if (projectionContracts.Contains(snapshot.ProjectionContractId) == false)
                return;

            var version = versionStore.Get(snapshot.ProjectionContractId.GetColumnFamily("_sp"));

            Save(snapshot, version.GetLiveVersionLocation());
        }

        private void Save(ISnapshot snapshot, string columnFamily)
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

        private void InitializeSnapshotsDatabase()
        {
            foreach (var projectionContractId in projectionContracts)
            {
                var version = versionStore.Get(projectionContractId.GetColumnFamily("_sp"));
                session.Execute(string.Format(TableTemplates.CreateSnapshopEventsTableTemplate, version.GetLiveVersionLocation()));
            }
        }

        private PreparedStatement BuildeInsertPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(InsertQueryTemplate, columnFamily));
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
    }
}
