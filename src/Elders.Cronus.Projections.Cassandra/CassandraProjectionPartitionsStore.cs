using Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Elders.Cronus.Projections.Cassandra
{
    public interface IProjectionPartionsStore
    {
        Task AppendAsync(ProjectionPartition record);

        /// <summary>
        /// Gets the list of all partitions for the specified projection instance.
        /// </summary>
        /// <param name="projectionName">The projection name.</param>
        /// <param name="projectionId">The projection instance ID.</param>
        /// <returns>Returns a sorted (ASC) list of all projection partitions.</returns>
        Task<List<IComparable<long>>> GetPartitionsAsync(string projectionName, IBlobId projectionId);
    }

    public class CassandraProjectionPartitionsStore : IProjectionPartionsStore
    {
        private const string Read = @"SELECT pid FROM ""{0}"".projection_partitions WHERE pt=? AND id=?;";
        private const string Write = @"INSERT INTO ""{0}"".projection_partitions (pt,id,pid) VALUES (?,?,?);";

        private const string ProjectionType = "pt";
        private const string ProjectionId = "id";
        private const string PartitionId = "pid";

        private readonly ICassandraProvider cassandraProvider;
        private readonly ILogger<CassandraProjectionPartitionsStore> logger;

        private readonly ConcurrentDictionary<string, PreparedStatement> ReadPreparedStatements;
        private readonly ConcurrentDictionary<string, PreparedStatement> WritePreparedStatements;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public CassandraProjectionPartitionsStore(ICassandraProvider cassandraProvider, ILogger<CassandraProjectionPartitionsStore> logger)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
            this.logger = logger;

            ReadPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            WritePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public async Task AppendAsync(ProjectionPartition record)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await GetWritePreparedStatementAsync(session).ConfigureAwait(false);

            var bs = statement.Bind(record.ProjectionName, record.ProjectionId, record.Partition).SetIdempotence(true);
            await session.ExecuteAsync(bs).ConfigureAwait(false);
        }

        public async Task<List<IComparable<long>>> GetPartitionsAsync(string projectionName, IBlobId projectionId)
        {
            List<IComparable<long>> partitions = new List<IComparable<long>>();

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await GetReadPreparedStatementAsync(session).ConfigureAwait(false);

            BoundStatement bs = statement.Bind(projectionName, projectionId.RawId);
            RowSet result = await session.ExecuteAsync(bs).ConfigureAwait(false);

            foreach (var row in result)
            {
                var loaded = row.GetValue<long>(PartitionId);
                partitions.Add(loaded);
            }

            return partitions;
        }

        private async Task<PreparedStatement> GetWritePreparedStatementAsync(ISession session)
        {
            if (WritePreparedStatements.TryGetValue(session.Keyspace, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(Write, session.Keyspace)).ConfigureAwait(false);
                statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                WritePreparedStatements.TryAdd(session.Keyspace, statement);
            }
            return statement;
        }

        private async Task<PreparedStatement> GetReadPreparedStatementAsync(ISession session)
        {
            if (ReadPreparedStatements.TryGetValue(session.Keyspace, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(Read, session.Keyspace)).ConfigureAwait(false);
                statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                ReadPreparedStatements.TryAdd(session.Keyspace, statement);
            }
            return statement;
        }
    }
}
