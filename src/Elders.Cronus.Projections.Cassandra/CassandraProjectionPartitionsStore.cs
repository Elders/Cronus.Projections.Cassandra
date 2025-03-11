using Cassandra;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;
using System;
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

    /// We tried to use <see cref="ISession.PrepareAsync(string, string)"/> where we wanted to specify the keyspace (we use [cqlsh 6.2.0 | Cassandra 5.0.2 | CQL spec 3.4.7 | Native protocol v5] cassandra)
    /// it seems like the driver does not have YET support for protocol v5 (still in beta). In code the driver is using protocol v4 (which is preventing us from using the above mentioned method)
    /// https://datastax-oss.atlassian.net/jira/software/c/projects/CSHARP/issues/CSHARP-856 as of 01.23.25 this epic is still in todo.
    public class CassandraProjectionPartitionsStore : IProjectionPartionsStore
    {
        private const string Read = @"SELECT pid FROM {0}.projection_partitions WHERE pt=? AND id=?;";
        private const string Write = @"INSERT INTO {0}.projection_partitions (pt,id,pid) VALUES (?,?,?);";

        private const string ProjectionType = "pt";
        private const string ProjectionId = "id";
        private const string PartitionId = "pid";

        private readonly ICassandraProvider cassandraProvider;
        private readonly ILogger<CassandraProjectionPartitionsStore> logger;

        private const string TableName = "projection_partitions";

        private ReadPreparedStatement _readPreparedStatement;
        private WritePreparedStatement _writePreparedStatement;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public CassandraProjectionPartitionsStore(ICronusContextAccessor cronusContextAccessor, ICassandraProvider cassandraProvider, ILogger<CassandraProjectionPartitionsStore> logger)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            _readPreparedStatement = new ReadPreparedStatement(cronusContextAccessor, cassandraProvider);
            _writePreparedStatement = new WritePreparedStatement(cronusContextAccessor, cassandraProvider);

            this.cassandraProvider = cassandraProvider;
            this.logger = logger;
        }

        public async Task AppendAsync(ProjectionPartition record)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await _writePreparedStatement.PrepareStatementAsync(session, TableName).ConfigureAwait(false);

            var bs = statement.Bind(record.ProjectionName, record.ProjectionId, record.Partition).SetIdempotence(true);
            await session.ExecuteAsync(bs).ConfigureAwait(false);
        }

        public async Task<List<IComparable<long>>> GetPartitionsAsync(string projectionName, IBlobId projectionId)
        {
            List<IComparable<long>> partitions = new List<IComparable<long>>();

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await _readPreparedStatement.PrepareStatementAsync(session, TableName).ConfigureAwait(false);

            BoundStatement bs = statement.Bind(projectionName, projectionId.RawId);
            RowSet result = await session.ExecuteAsync(bs).ConfigureAwait(false);

            foreach (var row in result)
            {
                var loaded = row.GetValue<long>(PartitionId);
                partitions.Add(loaded);
            }

            return partitions;
        }

        class ReadPreparedStatement : PreparedStatementCache
        {
            public ReadPreparedStatement(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
            { }
            internal override string GetQueryTemplate() => Read;
        }

        class WritePreparedStatement : PreparedStatementCache
        {
            public WritePreparedStatement(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
            { }
            internal override string GetQueryTemplate() => Write;
        }
    }
}
