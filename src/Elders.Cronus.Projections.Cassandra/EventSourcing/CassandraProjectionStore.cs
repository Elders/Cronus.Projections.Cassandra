using System.Collections.Generic;
using System;
using Cassandra;
using System.Collections.Concurrent;
using Elders.Cronus.Serializer;
using System.IO;
using System.Linq;
using Elders.Cronus.Projections.Cassandra.Logging;
using Elders.Cronus.Projections.Cassandra.Config;
using Elders.Cronus.Projections.Versioning;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class CassandraProjectionStore : IProjectionStore
    {
        static ILog log = LogProvider.GetLogger(typeof(CassandraProjectionStore));

        static readonly object createMutex = new object();
        static readonly object dropMutex = new object();

        const string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, sm int, evarid text, evarrev int, evarts bigint, evarpos int, data blob, PRIMARY KEY ((id, sm), evarid, evarrev, evarpos, evarts)) WITH CLUSTERING ORDER BY (evarid ASC);";
        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, sm, evarid, evarrev, evarpos, evarts, data) VALUES (?,?,?,?,?,?,?);";
        const string GetQueryTemplate = @"SELECT data FROM ""{0}"" WHERE id=? AND sm=?;";
        const string DropQueryTemplate = @"DROP TABLE IF EXISTS ""{0}"";";

        readonly ISession session;
        readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> CreatePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> DropPreparedStatements;
        readonly ISerializer serializer;
        private readonly IPublisher<ICommand> publisher;

        public CassandraProjectionStore(IEnumerable<Type> projections, ISession session, ISerializer serializer, IPublisher<ICommand> publisher)
        {
            if (ReferenceEquals(null, projections) == true) throw new ArgumentNullException(nameof(projections));
            if (ReferenceEquals(null, session) == true) throw new ArgumentNullException(nameof(session));
            if (ReferenceEquals(null, serializer) == true) throw new ArgumentNullException(nameof(serializer));

            this.serializer = serializer;
            this.publisher = publisher;
            this.session = session;
            this.SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.CreatePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.DropPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();

            log.Debug($"[{nameof(CassandraProjectionStore)}] Initialized with keyspace {session.Keyspace}");
        }

        public IEnumerable<ProjectionCommit> Load(ProjectionVersion version, IBlobId projectionId, int snapshotMarker)
        {
            var columnFamily = version.ProjectionName.GetColumnFamily(version);
            return Load(version.ProjectionName, projectionId, snapshotMarker, columnFamily);
        }

        IEnumerable<ProjectionCommit> Load(string contractId, IBlobId projectionId, int snapshotMarker, string columnFamily)
        {
            IEnumerable<Row> rows = null;
            try
            {
                string projId = Convert.ToBase64String(projectionId.RawId);

                BoundStatement bs = GetPreparedStatementToGetProjection(columnFamily).Bind(projId, snapshotMarker);
                var result = session.Execute(bs);
                rows = result.GetRows();
            }
            catch (InvalidQueryException)
            {
                CreateTable(CreateProjectionEventsTableTemplate, columnFamily);
                var id = new ProjectionVersionManagerId(contractId);
                var command = new RegisterProjection(id, contractId.GetTypeByContract().GetProjectionHash());
                publisher.Publish(command);
                throw;
            }

            foreach (var row in rows)
            {
                var data = row.GetValue<byte[]>("data");
                using (var stream = new MemoryStream(data))
                {
                    yield return (ProjectionCommit)serializer.Deserialize(stream);
                }
            }

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
            string projectionCommitLocationBasedOnVersion = commit.Version.ProjectionName.GetColumnFamily(commit.Version);
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

        PreparedStatement BuildInsertPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(InsertQueryTemplate, columnFamily));
        }

        PreparedStatement BuildDropPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(DropQueryTemplate, columnFamily));
        }

        PreparedStatement BuildCreatePreparedStatement(string template, string columnFamily)
        {
            return session.Prepare(string.Format(template, columnFamily));
        }

        public void InitializeProjectionStore(ProjectionVersion projectionVersion)
        {
            CreateTable(CreateProjectionEventsTableTemplate, projectionVersion.GetColumnFamily());
        }

        PreparedStatement GetPreparedStatementToGetProjection(string columnFamily)
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
