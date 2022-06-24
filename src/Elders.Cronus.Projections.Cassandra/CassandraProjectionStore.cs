using System.Collections.Generic;
using System;
using Cassandra;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStore<TSettings> : CassandraProjectionStore where TSettings : ICassandraProjectionStoreSettings
    {
        public CassandraProjectionStore(TSettings settings, ILogger<CassandraProjectionStore> logger) : base(settings.CassandraProvider, settings.Serializer, settings.ProjectionsNamingStrategy, logger) { }
    }

    public class CassandraProjectionStore : IProjectionStore
    {
        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, sm, evarid, evarrev, evarpos, evarts, data) VALUES (?,?,?,?,?,?,?);";
        const string GetQueryTemplate = @"SELECT data, evarid, evarrev, evarpos FROM ""{0}"" WHERE id=? AND sm=?;";
        const string HasSnapshotMarkerTemplate = @"SELECT sm FROM ""{0}"" WHERE id=? AND sm=? LIMIT 1;";

        private readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        private readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;
        private readonly ConcurrentDictionary<string, PreparedStatement> HasSnapshotMarkerPreparedStatements;

        private readonly ICassandraProvider cassandraProvider;
        private readonly ISerializer serializer;
        private readonly VersionedProjectionsNaming naming;
        private readonly ILogger<CassandraProjectionStore> logger;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public CassandraProjectionStore(ICassandraProvider cassandraProvider, ISerializer serializer, VersionedProjectionsNaming naming, ILogger<CassandraProjectionStore> logger)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            if (serializer is null) throw new ArgumentNullException(nameof(serializer));
            if (naming is null) throw new ArgumentNullException(nameof(naming));

            this.cassandraProvider = cassandraProvider;
            this.serializer = serializer;
            this.naming = naming;
            this.logger = logger;

            SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            HasSnapshotMarkerPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public async IAsyncEnumerable<ProjectionCommit> LoadAsync(ProjectionVersion version, IBlobId projectionId, int snapshotMarker)
        {
            string columnFamily = naming.GetColumnFamily(version);
            string projId = Convert.ToBase64String(projectionId.RawId);

            PreparedStatement preparedStatement = await GetPreparedStatementToGetProjectionAsync(columnFamily).ConfigureAwait(false);
            BoundStatement bs = preparedStatement.Bind(projId, snapshotMarker);

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            var rows = await session.ExecuteAsync(bs).ConfigureAwait(false);

            foreach (var row in rows)
            {
                byte[] data = row.GetValue<byte[]>(ProjectionColumn.EventData);

                if (data is not null)
                {
                    yield return (ProjectionCommit)serializer.DeserializeFromBytes(data);
                }
                else
                {
                    byte[] arid = row.GetValue<byte[]>(ProjectionColumn.EventAggregateId);
                    int revision = row.GetValue<int>(ProjectionColumn.EventAggregateRevision);
                    int position = row.GetValue<int>(ProjectionColumn.EventAggregateRevision);
                    logger.Error(() => $"Failed to load event `data` published by:{Environment.NewLine}aggregateId={arid}{Environment.NewLine}aggregateRevision={revision}{Environment.NewLine}position={position}{Environment.NewLine}projectionId={projId}");
                }
            }
        }

        public Task<bool> HasSnapshotMarkerAsync(ProjectionVersion version, IBlobId projectionId, int snapshotMarker)
        {
            string columnFamily = naming.GetColumnFamily(version);
            return HasSnapshotMarkerAsync(projectionId, snapshotMarker, columnFamily);
        }

        async Task<bool> HasSnapshotMarkerAsync(IBlobId projectionId, int snapshotMarker, string columnFamily)
        {
            string projId = Convert.ToBase64String(projectionId.RawId);
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement preparedStatement = await GetPreparedStatementToCheckProjectionSnapshotMarkerAsync(columnFamily).ConfigureAwait(false);
            BoundStatement bs = preparedStatement.Bind(projId, snapshotMarker);
            var result = await session.ExecuteAsync(bs).ConfigureAwait(false);
            IEnumerable<Row> rows = result.GetRows();

            return rows.Any();
        }

        public async IAsyncEnumerable<ProjectionCommit> EnumerateProjectionAsync(ProjectionVersion version, IBlobId projectionId)
        {
            int snapshotMarker = 0;
            snapshotMarker++;
            var loadedCommits = LoadAsync(version, projectionId, snapshotMarker).ConfigureAwait(false);
            await foreach (var loadedCommit in loadedCommits)
                yield return loadedCommit;
        }

        public Task SaveAsync(ProjectionCommit commit)
        {
            string projectionCommitLocationBasedOnVersion = naming.GetColumnFamily(commit.Version);
            return SaveAsync(commit, projectionCommitLocationBasedOnVersion);
        }

        async Task SaveAsync(ProjectionCommit commit, string columnFamily)
        {
            byte[] data = serializer.SerializeToBytes(commit);

            PreparedStatement newPreparedStatement = await BuildInsertPreparedStatemntAsync(columnFamily).ConfigureAwait(false);
            PreparedStatement statement = SavePreparedStatements.GetOrAdd(columnFamily, newPreparedStatement);

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            var result = await session.ExecuteAsync(statement
                .Bind(
                    ConvertIdToString(commit.ProjectionId),
                    commit.SnapshotMarker,
                    commit.EventOrigin.AggregateRootId,
                    commit.EventOrigin.AggregateRevision,
                    commit.EventOrigin.AggregateEventPosition,
                    commit.EventOrigin.Timestamp,
                    data
                )).ConfigureAwait(false);
        }

        async Task<PreparedStatement> BuildInsertPreparedStatemntAsync(string columnFamily)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await session.PrepareAsync(string.Format(InsertQueryTemplate, columnFamily));
            statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            return statement;
        }

        async Task<PreparedStatement> GetPreparedStatementToGetProjectionAsync(string columnFamily)
        {
            if (!GetPreparedStatements.TryGetValue(columnFamily, out PreparedStatement loadPreparedStatement))
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);

                loadPreparedStatement = await session.PrepareAsync(string.Format(GetQueryTemplate, columnFamily)).ConfigureAwait(false);
                loadPreparedStatement = loadPreparedStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                GetPreparedStatements.TryAdd(columnFamily, loadPreparedStatement);
            }
            return loadPreparedStatement;
        }

        async Task<PreparedStatement> GetPreparedStatementToCheckProjectionSnapshotMarkerAsync(string columnFamily)
        {
            if (!HasSnapshotMarkerPreparedStatements.TryGetValue(columnFamily, out PreparedStatement checkSnapshotMarkerPreparedStatement))
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);

                checkSnapshotMarkerPreparedStatement = await session.PrepareAsync(string.Format(HasSnapshotMarkerTemplate, columnFamily)).ConfigureAwait(false);
                checkSnapshotMarkerPreparedStatement = checkSnapshotMarkerPreparedStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                HasSnapshotMarkerPreparedStatements.TryAdd(columnFamily, checkSnapshotMarkerPreparedStatement);
            }
            return checkSnapshotMarkerPreparedStatement;
        }

        string ConvertIdToString(object id)
        {
            if (id is string || id is Guid)
                return id.ToString();

            if (id is IBlobId)
            {
                return Convert.ToBase64String((id as IBlobId).RawId);
            }
            throw new NotImplementedException(string.Format("Unknow type id {0}", id.GetType()));
        }
    }
}
