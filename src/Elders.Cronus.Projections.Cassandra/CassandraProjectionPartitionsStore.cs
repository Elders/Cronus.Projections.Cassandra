using Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Elders.Cronus.Projections.PartitionIndex;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionPartitionsStore : IProjectionPartionsStore
    {
        private const string Read = @"SELECT pt,id,pid FROM projection_partitions WHERE pt=? AND id=?;";
        private const string Write = @"INSERT INTO projection_partitions (pt,id,pid) VALUES (?,?,?);";

        private const string ProjectionType = "pt";
        private const string ProjectionId = "id";
        private const string PartitionId = "pid";

        private readonly ICassandraProvider cassandraProvider;
        private readonly ILogger<CassandraProjectionPartitionsStore> logger;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public CassandraProjectionPartitionsStore(ICassandraProvider cassandraProvider, ILogger<CassandraProjectionPartitionsStore> logger)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
            this.logger = logger;
        }

        public async Task AppendAsync(ProjectionPartition record)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await GetWritePreparedStatementAsync(session).ConfigureAwait(false);

            var bs = statement.Bind(record.ProjectionName, record.ProjectionId, record.Partition).SetIdempotence(true);
            await session.ExecuteAsync(bs).ConfigureAwait(false);
        }

        public async IAsyncEnumerable<ProjectionPartition> GetPartitionsAsync(string projectionName, IBlobId projectionId)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await GetReadPreparedStatementAsync(session).ConfigureAwait(false);

            BoundStatement bs = statement.Bind(projectionName, projectionId.RawId);
            RowSet result = await session.ExecuteAsync(bs).ConfigureAwait(false);

            foreach (var row in result)
            {
                yield return new ProjectionPartition(row.GetValue<string>(ProjectionType), row.GetValue<byte[]>(ProjectionId), row.GetValue<long>(PartitionId));
            }
        }

        private async Task<PreparedStatement> GetWritePreparedStatementAsync(ISession session)
        {
            PreparedStatement writeStatement = await session.PrepareAsync(Write).ConfigureAwait(false);
            writeStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

            return writeStatement;
        }

        private async Task<PreparedStatement> GetReadPreparedStatementAsync(ISession session)
        {
            PreparedStatement readStatement = await session.PrepareAsync(Read).ConfigureAwait(false);
            readStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

            return readStatement;
        }
    }
}
