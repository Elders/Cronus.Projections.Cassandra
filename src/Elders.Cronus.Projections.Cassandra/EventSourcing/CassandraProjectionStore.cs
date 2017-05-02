using Elders.Cronus.DomainModeling;
using System.Collections.Generic;
using System;
using Cassandra;
using System.Collections.Concurrent;
using Elders.Cronus.Serializer;
using System.IO;
using Elders.Cronus.Projections.Cassandra.Config;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class CassandraProjectionStore : IProjectionStore
    {
        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, sm, evarid, evarrev, evarpos, evarts, data) VALUES (?,?,?,?,?,?,?);";
        const string GetQueryTemplate = @"SELECT data FROM ""{0}"" WHERE id=? AND sm=?;";

        readonly ISession session;
        readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;
        readonly ISerializer serializer;

        public CassandraProjectionStore(ISession session, ISerializer serializer)
        {
            this.serializer = serializer;
            this.session = session;
            this.SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public ProjectionStream Load<T>(IBlobId projectionId, ISnapshot snapshot) where T : IProjectionDefinition
        {
            return Load(typeof(T), projectionId, snapshot);
        }

        public ProjectionStream Load(Type projectionType, IBlobId projectionId, ISnapshot snapshot)
        {
            string projId = Convert.ToBase64String(projectionId.RawId);
            List<ProjectionCommit> commits = new List<ProjectionCommit>();
            bool tryGetRecords = true;
            int snapshotMarker = snapshot.Revision;
            while (tryGetRecords)
            {
                tryGetRecords = false;
                BoundStatement bs = GetPreparedStatementToGetProjection(projectionType.GetColumnFamily()).Bind(projId, snapshotMarker);
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

            return new ProjectionStream(commits, snapshot);
        }

        public void Save(ProjectionCommit commit)
        {
            var data = serializer.SerializeToBytes(commit);
            var statement = SavePreparedStatements.GetOrAdd(commit.ProjectionType.GetColumnFamily(), x => BuildeInsertPreparedStatemnt(x));
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

        PreparedStatement BuildeInsertPreparedStatemnt(string columnFamily)
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

        string ConvertIdToString(object id)
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
