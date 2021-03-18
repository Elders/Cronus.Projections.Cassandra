using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Elders.Cronus.Projections.Snapshotting;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraSnapshotStore<TSettings> : CassandraSnapshotStore where TSettings : ICassandraProjectionStoreSettings
    {
        public CassandraSnapshotStore(TSettings settings) : base(settings.CassandraProvider, settings.Serializer, settings.ProjectionsNamingStrategy, settings.ProjectionsProvider) { }
    }

    public class CassandraSnapshotStore : ISnapshotStore
    {
        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, rev, data) VALUES (?,?,?);";
        const string GetQueryTemplate = @"SELECT data, rev FROM ""{0}"" WHERE id=? LIMIT 1;";
        const string GetSnapshotMetaQueryTemplate = @"SELECT rev FROM ""{0}"" WHERE id=? LIMIT 1;";

        readonly HashSet<string> projectionContracts;
        readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetSnapshotMetaPreparedStatements;
        readonly ISerializer serializer;
        private readonly VersionedProjectionsNaming naming;

        private ISession GetSession() => cassandraProvider.GetSession();

        private readonly ICassandraProvider cassandraProvider;

        public CassandraSnapshotStore(ICassandraProvider cassandraProvider, ISerializer serializer, VersionedProjectionsNaming naming, ProjectionsProvider projectionsProvider)
        {
            if (projectionsProvider is null) throw new ArgumentNullException(nameof(projectionsProvider));

            projectionContracts = new HashSet<string>(
                projectionsProvider.GetProjections()
                .Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x))
                .Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IEventHandler<>)))
                .Select(proj => proj.GetContractId()));

            this.cassandraProvider = cassandraProvider ?? throw new ArgumentNullException(nameof(cassandraProvider));
            this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            this.naming = naming ?? throw new ArgumentNullException(nameof(naming));

            SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetSnapshotMetaPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public ISnapshot Load(string projectionName, IBlobId id, ProjectionVersion version)
        {
            if (projectionContracts.Contains(projectionName) == false)
                return new NoSnapshot(id, projectionName);

            string columnFamily = naming.GetSnapshotColumnFamily(version);

            Row row = null;
            var bs = GetPreparedStatementToGetProjection(columnFamily).Bind(Convert.ToBase64String(id.RawId));
            var result = GetSession().Execute(bs);
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

            var bs = GetPreparedStatementToGetSnapshotMeta(columnFamily).Bind(Convert.ToBase64String(id.RawId));
            var result = GetSession().Execute(bs);
            Row row = result.GetRows().FirstOrDefault();

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

            var data = serializer.SerializeToBytes(snapshot.State);
            var statement = SavePreparedStatements.GetOrAdd(columnFamily, x => BuildeInsertPreparedStatemnt(x));
            var result = GetSession().Execute(statement
                .Bind(
                    Convert.ToBase64String(snapshot.Id.RawId),
                    snapshot.Revision,
                    data
                ));
        }

        PreparedStatement BuildeInsertPreparedStatemnt(string columnFamily)
        {
            return GetSession().Prepare(string.Format(InsertQueryTemplate, columnFamily));
        }

        PreparedStatement GetPreparedStatementToGetProjection(string columnFamily)
        {
            PreparedStatement statement = GetSession()
                .Prepare(string.Format(GetQueryTemplate, columnFamily))
                .SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

            return GetPreparedStatements.GetOrAdd(columnFamily, statement);
        }

        async Task<PreparedStatement> GetPreparedStatementToGetProjectionAsync(string columnFamily)
        {
            PreparedStatement statement = await GetSession().PrepareAsync(string.Format(GetQueryTemplate, columnFamily)).ConfigureAwait(false);
            statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

            return GetPreparedStatements.GetOrAdd(columnFamily, statement);
        }

        PreparedStatement GetPreparedStatementToGetSnapshotMeta(string columnFamily)
        {
            PreparedStatement statement = GetSession()
                .Prepare(string.Format(GetSnapshotMetaQueryTemplate, columnFamily))
                .SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            return GetSnapshotMetaPreparedStatements.GetOrAdd(columnFamily, statement);
        }

        async Task<PreparedStatement> GetPreparedStatementToGetSnapshotMetaAsync(string columnFamily)
        {
            var statement = await GetSession().PrepareAsync(string.Format(GetSnapshotMetaQueryTemplate, columnFamily)).ConfigureAwait(false);
            statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

            return GetSnapshotMetaPreparedStatements.GetOrAdd(columnFamily, statement);
        }

        public async Task<ISnapshot> LoadAsync(string projectionName, IBlobId id, ProjectionVersion version)
        {
            if (projectionContracts.Contains(projectionName) == false)
                return new NoSnapshot(id, projectionName);

            string columnFamily = naming.GetSnapshotColumnFamily(version);

            PreparedStatement preparedStatement = await GetPreparedStatementToGetProjectionAsync(columnFamily).ConfigureAwait(false);

            var bs = preparedStatement.Bind(Convert.ToBase64String(id.RawId));
            var result = await GetSession().ExecuteAsync(bs).ConfigureAwait(false);
            Row row = result.GetRows().FirstOrDefault();

            if (row == null)
                return new NoSnapshot(id, projectionName);

            var data = row.GetValue<byte[]>("data");
            var rev = row.GetValue<int>("rev");

            using (var stream = new MemoryStream(data))
            {
                return new Snapshot(id, projectionName, serializer.Deserialize(stream), rev);
            }
        }

        public async Task<SnapshotMeta> LoadMetaAsync(string projectionName, IBlobId id, ProjectionVersion version)
        {
            if (projectionContracts.Contains(projectionName) == false)
                return new NoSnapshot(id, projectionName).GetMeta();

            string columnFamily = naming.GetSnapshotColumnFamily(version);

            PreparedStatement preparedStatement = await GetPreparedStatementToGetSnapshotMetaAsync(columnFamily).ConfigureAwait(false);

            var bs = preparedStatement.Bind(Convert.ToBase64String(id.RawId));
            var result = await GetSession().ExecuteAsync(bs);
            Row row = result.GetRows().FirstOrDefault();

            if (row == null)
                return new NoSnapshot(id, projectionName).GetMeta();

            var rev = row.GetValue<int>("rev");

            return new SnapshotMeta(rev, projectionName);
        }
    }
}
