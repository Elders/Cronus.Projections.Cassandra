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

        private ISession GetSession() => cassandraProvider.GetSession(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

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

        public async Task<IEnumerable<ProjectionCommit>> LoadAsync(ProjectionVersion version, IBlobId projectionId, int snapshotMarker)
        {
            string columnFamily = naming.GetColumnFamily(version);
            string projId = Convert.ToBase64String(projectionId.RawId);

            PreparedStatement preparedStatement = await GetPreparedStatementToGetProjectionAsync(columnFamily).ConfigureAwait(false);
            BoundStatement bs = preparedStatement.Bind(projId, snapshotMarker);

            var rows = await GetSession().ExecuteAsync(bs).ConfigureAwait(false);

            var projectionCommits = new List<ProjectionCommit>();
            foreach (var row in rows)
            {
                byte[] data = row.GetValue<byte[]>(ProjectionColumn.EventData);

                if (data is not null)
                {
                    projectionCommits.Add((ProjectionCommit)serializer.DeserializeFromBytes(data));
                }
                else
                {
                    string arid = row.GetValue<string>(ProjectionColumn.EventAggregateId);
                    int revision = row.GetValue<int>(ProjectionColumn.EventAggregateRevision);
                    int position = row.GetValue<int>(ProjectionColumn.EventAggregateRevision);
                    logger.Error(() => $"Failed to load event `data` published by:{Environment.NewLine}aggregateId={arid}{Environment.NewLine}aggregateRevision={revision}{Environment.NewLine}position={position}{Environment.NewLine}projectionId={projId}");
                }
            }

            return projectionCommits;
        }

        public IEnumerable<ProjectionCommit> Load(ProjectionVersion version, IBlobId projectionId, int snapshotMarker)
        {
            string columnFamily = naming.GetColumnFamily(version);
            string projId = Convert.ToBase64String(projectionId.RawId);

            BoundStatement bs = GetPreparedStatementToGetProjection(columnFamily).Bind(projId, snapshotMarker);

            List<ProjectionCommit> cc = new List<ProjectionCommit>();

            IEnumerable<Row> rows = GetSession().Execute(bs);

            foreach (var row in rows)
            {
                byte[] data = row.GetValue<byte[]>(ProjectionColumn.EventData);

                if (data is not null)
                {
                    ProjectionCommit commit = ((ProjectionCommit)serializer.DeserializeFromBytes(data));
                    yield return commit;
                }
                else
                {
                    string arid = row.GetValue<string>(ProjectionColumn.EventAggregateId);
                    int revision = row.GetValue<int>(ProjectionColumn.EventAggregateRevision);
                    int position = row.GetValue<int>(ProjectionColumn.EventAggregateRevision);
                    logger.Error(() => $"Failed to load event `data` published by:{Environment.NewLine}aggregateId={arid}{Environment.NewLine}aggregateRevision={revision}{Environment.NewLine}position={position}{Environment.NewLine}projectionId={projId}");
                }
            }
        }

        public bool HasSnapshotMarker(ProjectionVersion version, IBlobId projectionId, int snapshotMarker)
        {
            string columnFamily = naming.GetColumnFamily(version);
            return HasSnapshotMarker(projectionId, snapshotMarker, columnFamily);
        }

        public Task<bool> HasSnapshotMarkerAsync(ProjectionVersion version, IBlobId projectionId, int snapshotMarker)
        {
            string columnFamily = naming.GetColumnFamily(version);
            return HasSnapshotMarkerAsync(projectionId, snapshotMarker, columnFamily);
        }

        bool HasSnapshotMarker(IBlobId projectionId, int snapshotMarker, string columnFamily)
        {
            string projId = Convert.ToBase64String(projectionId.RawId);

            BoundStatement bs = GetPreparedStatementToCheckProjectionSnapshotMarker(columnFamily).Bind(projId, snapshotMarker);
            var result = GetSession().Execute(bs);
            IEnumerable<Row> rows = result.GetRows();

            return rows.Any();
        }

        async Task<bool> HasSnapshotMarkerAsync(IBlobId projectionId, int snapshotMarker, string columnFamily)
        {
            string projId = Convert.ToBase64String(projectionId.RawId);

            PreparedStatement preparedStatement = await GetPreparedStatementToCheckProjectionSnapshotMarkerAsync(columnFamily);
            BoundStatement bs = preparedStatement.Bind(projId, snapshotMarker);
            var result = await GetSession().ExecuteAsync(bs).ConfigureAwait(false);
            IEnumerable<Row> rows = result.GetRows();

            return rows.Any();
        }

        public IEnumerable<ProjectionCommit> EnumerateProjection(ProjectionVersion version, IBlobId projectionId)
        {
            int snapshotMarker = 0;
            while (true)
            {
                snapshotMarker++;
                IEnumerable<ProjectionCommit> loadedCommits = Load(version, projectionId, snapshotMarker);
                foreach (var loadedCommit in loadedCommits)
                {
                    yield return loadedCommit;
                }

                if (loadedCommits.Any() == false)
                    break;
            }
        }

        public void Save(ProjectionCommit commit)
        {
            string projectionCommitLocationBasedOnVersion = naming.GetColumnFamily(commit.Version);
            Save(commit, projectionCommitLocationBasedOnVersion);
        }

        void Save(ProjectionCommit commit, string columnFamily)
        {
            var data = serializer.SerializeToBytes(commit);
            var statement = SavePreparedStatements.GetOrAdd(columnFamily, x => BuildInsertPreparedStatemnt(x));
            var result = GetSession().Execute(statement
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

        PreparedStatement BuildInsertPreparedStatemnt(string columnFamily)
        {
            return GetSession()
                .Prepare(string.Format(InsertQueryTemplate, columnFamily))
                .SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        }

        PreparedStatement GetPreparedStatementToGetProjection(string columnFamily)
        {
            if (!GetPreparedStatements.TryGetValue(columnFamily, out PreparedStatement loadPreparedStatement))
            {
                loadPreparedStatement = GetSession()
                    .Prepare(string.Format(GetQueryTemplate, columnFamily))
                    .SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                GetPreparedStatements.TryAdd(columnFamily, loadPreparedStatement);
            }
            return loadPreparedStatement;
        }

        PreparedStatement GetPreparedStatementToCheckProjectionSnapshotMarker(string columnFamily)
        {
            if (!HasSnapshotMarkerPreparedStatements.TryGetValue(columnFamily, out PreparedStatement checkSnapshotMarkerPreparedStatement))
            {
                checkSnapshotMarkerPreparedStatement = GetSession()
                    .Prepare(string.Format(HasSnapshotMarkerTemplate, columnFamily))
                    .SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                HasSnapshotMarkerPreparedStatements.TryAdd(columnFamily, checkSnapshotMarkerPreparedStatement);
            }
            return checkSnapshotMarkerPreparedStatement;
        }

        async Task<PreparedStatement> GetPreparedStatementToCheckProjectionSnapshotMarkerAsync(string columnFamily)
        {
            if (!HasSnapshotMarkerPreparedStatements.TryGetValue(columnFamily, out PreparedStatement checkSnapshotMarkerPreparedStatement))
            {
                checkSnapshotMarkerPreparedStatement = await GetSession().PrepareAsync(string.Format(HasSnapshotMarkerTemplate, columnFamily)).ConfigureAwait(false);
                checkSnapshotMarkerPreparedStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                HasSnapshotMarkerPreparedStatements.TryAdd(columnFamily, checkSnapshotMarkerPreparedStatement);
            }
            return checkSnapshotMarkerPreparedStatement;
        }

        async Task<PreparedStatement> GetPreparedStatementToGetProjectionAsync(string columnFamily)
        {
            if (!GetPreparedStatements.TryGetValue(columnFamily, out PreparedStatement loadAggregatePreparedStatement))
            {
                loadAggregatePreparedStatement = await GetSession().PrepareAsync(string.Format(GetQueryTemplate, columnFamily)).ConfigureAwait(false);
                loadAggregatePreparedStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                GetPreparedStatements.TryAdd(columnFamily, loadAggregatePreparedStatement);
            }
            return loadAggregatePreparedStatement;
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
