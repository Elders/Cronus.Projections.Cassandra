using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Elders.Cronus.Projections.Snapshotting;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraSnapshotStore<TSettings> : CassandraSnapshotStore where TSettings : ICassandraProjectionStoreSettings
    {
        public CassandraSnapshotStore(TSettings settings, IInitializableProjectionStore initializableProjectionStore, ILogger<CassandraSnapshotStore> logger) : base(settings.CassandraProvider, settings.Serializer, settings.ProjectionsNamingStrategy, settings.ProjectionsProvider, initializableProjectionStore, logger) { }
    }

    public class CassandraSnapshotStore : ISnapshotStore
    {
        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, rev, data) VALUES (?,?,?);";
        const string GetQueryTemplate = @"SELECT data, rev FROM ""{0}"" WHERE id=? LIMIT 1;";
        const string GetSnapshotMetaQueryTemplate = @"SELECT rev FROM ""{0}"" WHERE id=? LIMIT 1;";

        private readonly ILogger<CassandraSnapshotStore> logger;
        private readonly HashSet<string> projectionContracts;
        private readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        private readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;
        private readonly ConcurrentDictionary<string, PreparedStatement> GetSnapshotMetaPreparedStatements;
        private readonly ISerializer serializer;
        private readonly VersionedProjectionsNaming naming;
        private readonly IInitializableProjectionStore initializableProjectionStore;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();

        private readonly ICassandraProvider cassandraProvider;

        public CassandraSnapshotStore(ICassandraProvider cassandraProvider, ISerializer serializer, VersionedProjectionsNaming naming, ProjectionsProvider projectionsProvider, IInitializableProjectionStore initializableProjectionStore, ILogger<CassandraSnapshotStore> logger)
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
            this.initializableProjectionStore = initializableProjectionStore;
            SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetSnapshotMetaPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.logger = logger;
        }

        public async Task<ISnapshot> LoadAsync(string projectionName, IBlobId id, ProjectionVersion version)
        {
            try
            {
                if (projectionContracts.Contains(projectionName) == false)
                    return new NoSnapshot(id, projectionName);

                string columnFamily = naming.GetSnapshotColumnFamily(version);

                Row row = null;
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                var bs = await GetPreparedStatementToGetProjectionAsync(columnFamily, session).ConfigureAwait(false);
                var result = await session.ExecuteAsync(bs.Bind(Convert.ToBase64String(id.RawId))).ConfigureAwait(false);
                row = result.GetRows().FirstOrDefault();

                if (row == null)
                    return new NoSnapshot(id, projectionName);

                var data = row.GetValue<byte[]>("data");
                var rev = row.GetValue<int>("rev");

                return new Snapshot(id, projectionName, serializer.DeserializeFromBytes<object>(data), rev);
            }
            catch (Exception)
            {
                await initializableProjectionStore.InitializeAsync(version).ConfigureAwait(false);

                return new NoSnapshot(id, projectionName);
            }
        }

        public async Task<SnapshotMeta> LoadMetaAsync(string projectionName, IBlobId id, ProjectionVersion version)
        {
            try
            {
                if (projectionContracts.Contains(projectionName) == false)
                    return new NoSnapshot(id, projectionName).GetMeta();

                string columnFamily = naming.GetSnapshotColumnFamily(version);

                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement statement = await GetPreparedStatementToGetSnapshotMetaAsync(columnFamily, session).ConfigureAwait(false);
                BoundStatement bs = statement.Bind(Convert.ToBase64String(id.RawId));
                var result = await session.ExecuteAsync(bs).ConfigureAwait(false);
                Row row = result.GetRows().FirstOrDefault();

                if (row == null)
                    return new NoSnapshot(id, projectionName).GetMeta();

                var rev = row.GetValue<int>("rev");

                return new SnapshotMeta(rev, projectionName);
            }
            catch (Exception)
            {
                await initializableProjectionStore.InitializeAsync(version).ConfigureAwait(false);
                return new NoSnapshot(id, projectionName).GetMeta();
            }
        }

        public async Task SaveAsync(ISnapshot snapshot, ProjectionVersion version)
        {
            string columnFamily = naming.GetSnapshotColumnFamily(version);

            try
            {
                if (projectionContracts.Contains(snapshot.ProjectionName) == false)
                    return;

                var data = serializer.SerializeToBytes(snapshot.State);

                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement statement = await GetInsertPreparedStatementAsync(columnFamily, session).ConfigureAwait(false);

                BoundStatement boundedStatement = statement.Bind(Convert.ToBase64String(snapshot.Id.RawId), snapshot.Revision, data);

                RowSet result = await session.ExecuteAsync(boundedStatement).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.ErrorException(ex, () => $"Failed to save snapshot to {columnFamily}.");
            }
        }

        async Task<PreparedStatement> GetInsertPreparedStatementAsync(string columnFamily, ISession session)
        {
            if (GetPreparedStatements.TryGetValue(columnFamily, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(InsertQueryTemplate, columnFamily)).ConfigureAwait(false);
                statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                GetPreparedStatements.TryAdd(columnFamily, statement);
            }

            return statement;
        }

        async Task<PreparedStatement> GetPreparedStatementToGetProjectionAsync(string columnFamily, ISession session)
        {
            if (GetPreparedStatements.TryGetValue(columnFamily, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(GetQueryTemplate, columnFamily)).ConfigureAwait(false);
                statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                GetPreparedStatements.TryAdd(columnFamily, statement);
            }

            return statement;
        }

        async Task<PreparedStatement> GetPreparedStatementToGetSnapshotMetaAsync(string columnFamily, ISession session)
        {
            if (GetSnapshotMetaPreparedStatements.TryGetValue(columnFamily, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(GetSnapshotMetaQueryTemplate, columnFamily)).ConfigureAwait(false);
                statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                GetSnapshotMetaPreparedStatements.TryAdd(columnFamily, statement);
            }

            return statement;
        }
    }
}
