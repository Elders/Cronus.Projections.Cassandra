using System.Collections.Generic;
using System;
using Cassandra;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using Elders.Cronus.Projections.Cassandra.Logging;
using System.Threading.Tasks;

namespace Elders.Cronus.Projections.Cassandra
{
    public sealed class CassandraProjectionStore : IProjectionStore, IInitializableProjectionStore
    {
        static ILog log = LogProvider.GetLogger(typeof(CassandraProjectionStore));

        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id, sm, evarid, evarrev, evarpos, evarts, data) VALUES (?,?,?,?,?,?,?);";
        const string GetQueryTemplate = @"SELECT data FROM ""{0}"" WHERE id=? AND sm=?;";


        private readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        private readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;

        private readonly ISerializer serializer;
        private readonly IProjectionsNamingStrategy naming;
        private readonly ISession session;
        private readonly CassandraProjectionStoreSchema projectionsSchema;
        private readonly CassandraSnapshotStoreSchema snapshotsSchema;


        public CassandraProjectionStore(ICassandraProvider cassandraProvider, ISerializer serializer, IProjectionsNamingStrategy naming, CassandraProjectionStoreSchema projectionsSchema, CassandraSnapshotStoreSchema snapshotsSchema)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            if (serializer is null) throw new ArgumentNullException(nameof(serializer));
            if (naming is null) throw new ArgumentNullException(nameof(naming));
            if (projectionsSchema is null) throw new ArgumentNullException(nameof(projectionsSchema));
            if (snapshotsSchema is null) throw new ArgumentNullException(nameof(snapshotsSchema));

            this.session = cassandraProvider.GetSession();
            this.serializer = serializer;
            this.naming = naming;
            this.projectionsSchema = projectionsSchema;
            this.snapshotsSchema = snapshotsSchema;

            SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public async Task<IEnumerable<ProjectionCommit>> LoadAsync(ProjectionVersion version, IBlobId projcetionId, int snapshotMarker)
        {
            string columnFamily = naming.GetColumnFamily(version);
            return await LoadAsync(version.ProjectionName, projcetionId, snapshotMarker, columnFamily);
        }

        public IEnumerable<ProjectionCommit> Load(ProjectionVersion version, IBlobId projectionId, int snapshotMarker)
        {
            string columnFamily = naming.GetColumnFamily(version);
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
                //projectionStoreStorageManager?.CreateProjectionsStorage(columnFamily);
                //var id = new ProjectionVersionManagerId(contractId);
                //var command = new RegisterProjection(id, contractId.GetTypeByContract().GetProjectionHash());
                //publisher.Publish(command);
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
            throw new NotImplementedException(string.Format("Unknow type id {0}", id.GetType()));
        }

        public void Initialize(ProjectionVersion version)
        {
            string projectionColumnFamily = naming.GetColumnFamily(version);
            log.Debug(() => $"[Projection Store] Initializing projection store with column family `{projectionColumnFamily}`...");
            projectionsSchema.CreateProjectionsStorage(projectionColumnFamily);
            log.Debug(() => $"[Projection Store] Initialized projection store with column family `{projectionColumnFamily}`");

            string snapshotColumnFamily = naming.GetSnapshotColumnFamily(version);
            log.Debug(() => $"[Snapshot Store] Initializing snapshot store with column family `{snapshotColumnFamily}`....");
            snapshotsSchema.CreateTable(snapshotColumnFamily);
            log.Debug(() => $"[Snapshot Store] Initialized snapshot store with column family `{snapshotColumnFamily}`");
        }
    }
}
