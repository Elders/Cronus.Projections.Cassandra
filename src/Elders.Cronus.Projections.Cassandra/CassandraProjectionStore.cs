﻿using System.Collections.Generic;
using System;
using Cassandra;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Elders.Cronus.Projections.Cassandra.Infrastructure;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStore<TSettings> : CassandraProjectionStore where TSettings : ICassandraProjectionStoreSettings
    {
        public CassandraProjectionStore(TSettings settings) : base(settings.CassandraProvider, settings.Serializer, settings.ProjectionsNamingStrategy) { }
    }

    public class CassandraProjectionStore : IProjectionStore
    {
        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, sm, evarid, evarrev, evarpos, evarts, data) VALUES (?,?,?,?,?,?,?);";
        const string GetQueryTemplate = @"SELECT data FROM ""{0}"" WHERE id=? AND sm=?;";


        private readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        private readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;

        private readonly ISerializer serializer;
        private readonly IProjectionsNamingStrategy naming;
        private readonly ISession session;

        public CassandraProjectionStore(ICassandraProvider cassandraProvider, ISerializer serializer, IProjectionsNamingStrategy naming)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            if (serializer is null) throw new ArgumentNullException(nameof(serializer));
            if (naming is null) throw new ArgumentNullException(nameof(naming));

            this.session = cassandraProvider.GetSession();
            this.serializer = serializer;
            this.naming = naming;

            SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public async Task<IEnumerable<ProjectionCommit>> LoadAsync(ProjectionVersion version, IBlobId projectionId, int snapshotMarker)
        {
            string columnFamily = naming.GetColumnFamily(version);
            return await LoadAsync(projectionId, snapshotMarker, columnFamily);
        }

        public IEnumerable<ProjectionCommit> Load(ProjectionVersion version, IBlobId projectionId, int snapshotMarker)
        {
            string columnFamily = naming.GetColumnFamily(version);
            return Load(projectionId, snapshotMarker, columnFamily);
        }

        IEnumerable<ProjectionCommit> Load(IBlobId projectionId, int snapshotMarker, string columnFamily)
        {
            string projId = Convert.ToBase64String(projectionId.RawId);

            BoundStatement bs = GetPreparedStatementToGetProjection(columnFamily).Bind(projId, snapshotMarker);
            var result = session.Execute(bs);
            IEnumerable<Row> rows = result.GetRows();

            foreach (var row in rows)
            {
                var data = row.GetValue<byte[]>("data");
                using (var stream = new MemoryStream(data))
                {
                    yield return (ProjectionCommit)serializer.Deserialize(stream);
                }
            }
        }

        async Task<IEnumerable<ProjectionCommit>> LoadAsync(IBlobId projectionId, int snapshotMarker, string columnFamily)
        {
            string projId = Convert.ToBase64String(projectionId.RawId);

            PreparedStatement preparedStatement = await GetPreparedStatementToGetProjectionAsync(columnFamily).ConfigureAwait(false);
            BoundStatement bs = preparedStatement.Bind(projId, snapshotMarker);

            var result = await session.ExecuteAsync(bs).ConfigureAwait(false);
            IEnumerable<Row> rows = result.GetRows();

            var projectionCommits = new List<ProjectionCommit>();
            foreach (var row in rows)
            {
                var data = row.GetValue<byte[]>("data");
                using (var stream = new MemoryStream(data))
                {
                    projectionCommits.Add((ProjectionCommit)serializer.Deserialize(stream));
                }
            }

            return projectionCommits;
        }

        public IEnumerable<ProjectionCommit> EnumerateProjection(ProjectionVersion version, IBlobId projectionId)
        {
            int snapshotMarker = 0;
            while (true)
            {
                snapshotMarker++;
                var loadedCommits = Load(version, projectionId, snapshotMarker);
                foreach (var loadedCommit in loadedCommits)
                {
                    yield return loadedCommit;
                }

                if (loadedCommits.Count() == 0)
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

        PreparedStatement BuildInsertPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(InsertQueryTemplate, columnFamily));
        }

        PreparedStatement GetPreparedStatementToGetProjection(string columnFamily)
        {
            if (!GetPreparedStatements.TryGetValue(columnFamily, out PreparedStatement loadAggregatePreparedStatement))
            {
                loadAggregatePreparedStatement = session.Prepare(string.Format(GetQueryTemplate, columnFamily));
                GetPreparedStatements.TryAdd(columnFamily, loadAggregatePreparedStatement);
            }
            return loadAggregatePreparedStatement;
        }

        async Task<PreparedStatement> GetPreparedStatementToGetProjectionAsync(string columnFamily)
        {
            if (!GetPreparedStatements.TryGetValue(columnFamily, out PreparedStatement loadAggregatePreparedStatement))
            {
                loadAggregatePreparedStatement = await session.PrepareAsync(string.Format(GetQueryTemplate, columnFamily)).ConfigureAwait(false);
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
