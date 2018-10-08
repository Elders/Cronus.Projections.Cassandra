using System.Collections.Generic;
using System;
using Cassandra;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using Elders.Cronus.Projections.Cassandra.Logging;
using Elders.Cronus.Projections.Cassandra.Config;
using Elders.Cronus.Projections.Versioning;
using System.Threading.Tasks;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class CassandraProjectionStore : IProjectionStore
    {
        static ILog log = LogProvider.GetLogger(typeof(CassandraProjectionStore));

        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, sm, evarid, evarrev, evarpos, evarts, data) VALUES (?,?,?,?,?,?,?);";
        const string GetQueryTemplate = @"SELECT data FROM ""{0}"" WHERE id=? AND sm=?;";


        readonly ISession session;
        readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;

        readonly ISerializer serializer;
        private readonly IPublisher<ICommand> publisher;
        private readonly CassandraProjectionStoreSchema schema;

        public CassandraProjectionStore(ISession session, ISerializer serializer, IPublisher<ICommand> publisher, CassandraProjectionStoreSchema schema)
        {
            if (ReferenceEquals(null, session) == true) throw new ArgumentNullException(nameof(session));
            if (ReferenceEquals(null, serializer) == true) throw new ArgumentNullException(nameof(serializer));
            if (ReferenceEquals(null, publisher) == true) throw new ArgumentNullException(nameof(publisher));

            this.session = session;
            this.serializer = serializer;
            this.publisher = publisher;
            this.schema = schema;

            SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();

            log.Debug($"[{nameof(CassandraProjectionStore)}] Initialized with keyspace {session.Keyspace}");
        }

        public CassandraProjectionStore(CassandraProvider cassandraProvider, ISerializer serializer, IPublisher<ICommand> publisher, CassandraProjectionStoreSchema schema)
            : this(cassandraProvider.GetSession(), serializer, publisher, schema)
        {

        }

        public async Task<IEnumerable<ProjectionCommit>> LoadAsync(ProjectionVersion version, IBlobId projcetionId, int snapshotMarker)
        {
            var columnFamily = version.ProjectionName.GetColumnFamily(version);
            return await LoadAsync(version.ProjectionName, projcetionId, snapshotMarker, columnFamily);
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
                schema?.CreateTable(columnFamily);
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

        async Task<IEnumerable<ProjectionCommit>> LoadAsync(string contractId, IBlobId projectionId, int snapshotMarker, string columnFamily)
        {
            IEnumerable<Row> rows = null;
            try
            {
                string projId = Convert.ToBase64String(projectionId.RawId);

                BoundStatement bs = GetPreparedStatementToGetProjection(columnFamily).Bind(projId, snapshotMarker);
                var result = await session.ExecuteAsync(bs);
                rows = result.GetRows();
            }
            catch (InvalidQueryException)
            {
                schema?.CreateTable(columnFamily);
                var id = new ProjectionVersionManagerId(contractId);
                var command = new RegisterProjection(id, contractId.GetTypeByContract().GetProjectionHash());
                publisher.Publish(command);
                throw;
            }

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

        PreparedStatement BuildInsertPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(InsertQueryTemplate, columnFamily));
        }

        public void InitializeProjectionStore(ProjectionVersion projectionVersion)
        {
            if (ReferenceEquals(null, schema) == false)
            {
                log.Debug(() => $"Initializing projection store with column family '{projectionVersion.GetColumnFamily()}' for projection version '{projectionVersion}'");
                schema.CreateTable(projectionVersion.GetColumnFamily());
                return;
            }

            log.Debug(() => "This node can not change Cassandra schema.");
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
