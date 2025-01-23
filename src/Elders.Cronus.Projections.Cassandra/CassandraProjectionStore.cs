using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.Persistence.Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    [Obsolete("Will be removed in v11")]
    public interface IProjectionStoreLegacy
    {
        IAsyncEnumerable<ProjectionCommit> LoadAsync(ProjectionVersion version, IBlobId projectionId);

        Task EnumerateProjectionsAsync(ProjectionsOperator @operator, ProjectionQueryOptions options);

        Task SaveAsync(ProjectionCommit commit);
    }

    public class CassandraProjectionStore<TSettings> : CassandraProjectionStore where TSettings : ICassandraProjectionStoreSettings
    {
        public CassandraProjectionStore(TSettings settings, ILogger<CassandraProjectionStore> logger) : base(settings.CassandraProvider, settings.Serializer, settings.ProjectionsNamingStrategy, logger) { }
    }

    /// We tried to use <see cref="ISession.PrepareAsync(string, string)"/> where we wanted to specify the keyspace (we use [cqlsh 6.2.0 | Cassandra 5.0.2 | CQL spec 3.4.7 | Native protocol v5] cassandra)
    /// it seems like the driver does not have YET support for protocol v5 (still in beta). In code the driver is using protocol v4 (which is preventing us from using the above mentioned method)
    /// https://datastax-oss.atlassian.net/jira/software/c/projects/CSHARP/issues/CSHARP-856 as of 01.23.25 this epic is still in todo.
    [Obsolete("Will be removed in v11")]
    public class CassandraProjectionStore : IProjectionStoreLegacy
    {
        // New Projection tables --->
        const string InsertNewQueryTemplate = @"INSERT INTO ""{0}"".""{1}"" (id,pid,data,ts) VALUES (?,?,?,?);";

        // Projection tables --->
        const string InsertQueryTemplate = @"INSERT INTO ""{0}"".""{1}"" (id,data,ts) VALUES (?,?,?);";
        const string GetQueryTemplate = @"SELECT data,ts FROM ""{0}"".""{1}"" WHERE id=?;";
        const string GetQueryAsOfTemplate = @"SELECT data,ts FROM ""{0}"".""{1}"" WHERE id=? AND ts<=?;";
        const string GetQueryDescendingTemplate = @"SELECT data,ts FROM ""{0}"".""{1}"" WHERE id=? order by ts desc";

        // Partition table --->
        private const string InsertPartition = @"INSERT INTO ""{0}"".projection_partitions (pt,id,pid) VALUES (?,?,?);";

        private readonly ICassandraProvider cassandraProvider;
        private readonly ISerializer serializer;
        private readonly VersionedProjectionsNaming naming;
        private readonly ILogger<CassandraProjectionStore> logger;

        public static EventId CronusProjectionEventLoadError = new EventId(74300, "CronusProjectionEventLoadError");
        private static readonly Action<ILogger, string, string, Exception> LogError = LoggerMessage.Define<string, string>(LogLevel.Error, CronusProjectionEventLoadError, "Failed to load event data. Handler -> {cronus_projection_type} Projection id -> {cronus_projection_id}");

        private readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatementsNew;
        private readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatementsLegacy;
        private readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatementsLegacy;
        private readonly ConcurrentDictionary<string, PreparedStatement> GetAsOfDatePreparedStatementsLegacy;
        private readonly ConcurrentDictionary<string, PreparedStatement> GetDescendingPreparedStatementsLegacy;
        private readonly ConcurrentDictionary<string, PreparedStatement> InsertPartitionPreparedStatements;

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

            SavePreparedStatementsNew = new ConcurrentDictionary<string, PreparedStatement>();
            SavePreparedStatementsLegacy = new ConcurrentDictionary<string, PreparedStatement>();
            GetPreparedStatementsLegacy = new ConcurrentDictionary<string, PreparedStatement>();
            GetAsOfDatePreparedStatementsLegacy = new ConcurrentDictionary<string, PreparedStatement>();
            GetDescendingPreparedStatementsLegacy = new ConcurrentDictionary<string, PreparedStatement>();
            InsertPartitionPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public async IAsyncEnumerable<ProjectionCommit> LoadAsync(ProjectionVersion version, IBlobId projectionId)
        {
            string columnFamily = naming.GetColumnFamily(version);

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement preparedStatement = await GetPreparedStatementToGetProjectionAsync(columnFamily, session).ConfigureAwait(false);
            BoundStatement bs = preparedStatement.Bind(projectionId.RawId);

            var rows = await session.ExecuteAsync(bs).ConfigureAwait(false);

            foreach (var row in rows)
            {
                byte[] data = row.GetValue<byte[]>(ProjectionColumn.EventData);

                if (data is not null)
                {
                    IEvent @event = serializer.DeserializeFromBytes<IEvent>(data);
                    yield return new ProjectionCommit(projectionId, version, @event);
                }
                else
                {
                    LogError(logger, version.ProjectionName, Convert.ToBase64String(projectionId.RawId.Span), null);
                }
            }
        }

        public async Task EnumerateProjectionsAsync(ProjectionsOperator @operator, ProjectionQueryOptions options)
        {
            if (options.AsOf.HasValue)
            {
                await EnumerateProjectionsAsOfDate(@operator, options).ConfigureAwait(false);
            }
            else if (options.PagingOptions is not null)
            {
                await EnumerateWithPagingAsync(@operator, options).ConfigureAwait(false);
            }
        }

        public async Task SaveAsync(ProjectionCommit commit)
        {
            string projectionCommitLocationBasedOnVersionLEGACY = naming.GetColumnFamily(commit.Version);
            string projectionCommitLocationBasedOnVersionNEW = naming.GetColumnFamilyNew(commit.Version);

            byte[] data = serializer.SerializeToBytes(commit.Event);

            ISession session = await GetSessionAsync().ConfigureAwait(false);

            BatchStatement batch = new BatchStatement();
            batch.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            batch.SetIdempotence(false);
            batch.SetBatchType(BatchType.Logged);

            long partitionId = CalculatePartition(commit.Event);
            byte[] projectionId = commit.ProjectionId.RawId.ToArray(); // the Bind() method invokes the driver serializers for each value

            // old projections
            PreparedStatement projectionStatement = await BuildInsertPreparedStatementAsync(projectionCommitLocationBasedOnVersionLEGACY, session).ConfigureAwait(false);
            BoundStatement projectionBoundStatement = projectionStatement.Bind(projectionId, data, commit.Event.Timestamp.ToFileTime());
            batch.Add(projectionBoundStatement);

            // new projections
            PreparedStatement projectionStatementNew = await BuildNewInsertPreparedStatementAsync(projectionCommitLocationBasedOnVersionNEW, session).ConfigureAwait(false);
            BoundStatement projectionBoundStatementNew = projectionStatementNew.Bind(projectionId, partitionId, data, commit.Event.Timestamp.ToFileTime());
            batch.Add(projectionBoundStatementNew);

            // partitions
            PreparedStatement partitionStatement = await GetWritePartitionsPreparedStatementAsync(session).ConfigureAwait(false);
            BoundStatement partitionBoundStatement = partitionStatement.Bind(commit.Version.ProjectionName, projectionId, partitionId);
            batch.Add(partitionBoundStatement);

            await session.ExecuteAsync(batch).ConfigureAwait(false);
        }

        async Task EnumerateProjectionsAsOfDate(ProjectionsOperator @operator, ProjectionQueryOptions options)
        {
            List<IEvent> events = new List<IEvent>();
            if (@operator.OnProjectionStreamLoadedAsync is not null)
            {
                await foreach (var @event in LoadAsOfDateInternalAsync(options))
                {
                    events.Add(@event);
                }

                var stream = new ProjectionStream(options.Version, options.Id, events);
                await @operator.OnProjectionStreamLoadedAsync(stream);
            }
        }

        async Task EnumerateWithPagingAsync(ProjectionsOperator @operator, ProjectionQueryOptions options)
        {
            if (@operator.OnProjectionStreamLoadedAsync is not null)
            {
                ProjectionStream stream = await EnumerateEntireProjectionStreamAsync(options).ConfigureAwait(false);

                await @operator.OnProjectionStreamLoadedAsync(stream).ConfigureAwait(false);
            }
            else if (@operator.OnProjectionStreamLoadedWithPagingAsync is not null)
            {
                PagingProjectionsResult result = await EnumerateWithPagingInternalAsync(options).ConfigureAwait(false);

                var pagedStream = new ProjectionStream(options.Version, options.Id, result.Events);
                var pagedOptions = new PagingOptions(options.PagingOptions.Take, result.NewPagingToken, options.PagingOptions.Order);
                await @operator.OnProjectionStreamLoadedWithPagingAsync(pagedStream, pagedOptions).ConfigureAwait(false);
            }
        }

        async IAsyncEnumerable<IEvent> LoadAsOfDateInternalAsync(ProjectionQueryOptions options)
        {
            string columnFamily = naming.GetColumnFamily(options.Version);

            PagingInfo pagingInfo = new PagingInfo();
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement preparedStatement = await GetAsOfDatePreparedStatementAsync(columnFamily, session).ConfigureAwait(false);

            IStatement bs = preparedStatement.Bind(options.Id.RawId, options.AsOf.Value.ToFileTime())
                                                  .SetPageSize(options.BatchSize)
                                                  .SetAutoPage(false);
            while (pagingInfo.HasMore)
            {
                if (pagingInfo.HasToken())
                    bs.SetPagingState(pagingInfo.Token);

                var rows = await session.ExecuteAsync(bs).ConfigureAwait(false);
                foreach (var row in rows)
                {
                    byte[] data = row.GetValue<byte[]>(ProjectionColumn.EventData);

                    if (data is not null)
                    {
                        IEvent @event = serializer.DeserializeFromBytes<IEvent>(data);
                        yield return @event;
                    }
                    else
                    {
                        LogError(logger, options.Version.ProjectionName, Convert.ToBase64String(options.Id.RawId.Span), null);
                    }
                }
                pagingInfo = PagingInfo.From(rows);
            }
        }

        async Task<PagingProjectionsResult> EnumerateWithPagingInternalAsync(ProjectionQueryOptions options)
        {
            PreparedStatement preparedStatement;
            PagingProjectionsResult pagingResult = new PagingProjectionsResult();

            string columnFamily = naming.GetColumnFamily(options.Version);
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            if (options.PagingOptions.Order.Equals(Order.Descending))
            {
                preparedStatement = await GetDescendingPreparedStatementAsync(columnFamily, session).ConfigureAwait(false);
            }
            else
            {
                preparedStatement = await GetPreparedStatementToGetProjectionAsync(columnFamily, session).ConfigureAwait(false);
            }

            IStatement boundStatement = preparedStatement.Bind(options.Id.RawId).SetPageSize(options.BatchSize).SetAutoPage(false);
            if (options.PagingOptions is not null)
            {
                boundStatement.SetPagingState(options.PagingOptions.PaginationToken);
            }

            RowSet result = await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            foreach (var row in result)
            {
                byte[] data = row.GetValue<byte[]>(ProjectionColumn.EventData);
                if (data is not null)
                {
                    IEvent @event = serializer.DeserializeFromBytes<IEvent>(data);
                    pagingResult.Events.Add(@event);
                }
                else
                {
                    LogError(logger, options.Version.ProjectionName, Convert.ToBase64String(options.Id.RawId.Span), null);
                }
            }
            pagingResult.NewPagingToken = result.PagingState;
            return pagingResult;
        }

        async Task<ProjectionStream> EnumerateEntireProjectionStreamAsync(ProjectionQueryOptions options)
        {
            PreparedStatement preparedStatement;
            PagingInfo pagingInfo = new PagingInfo();

            List<IEvent> events = new List<IEvent>();

            string columnFamily = naming.GetColumnFamily(options.Version);
            ISession session = await GetSessionAsync().ConfigureAwait(false);

            if (options.PagingOptions.Order.Equals(Order.Descending))
                preparedStatement = await GetDescendingPreparedStatementAsync(columnFamily, session).ConfigureAwait(false);
            else
                preparedStatement = await GetPreparedStatementToGetProjectionAsync(columnFamily, session).ConfigureAwait(false);

            IStatement bs = preparedStatement.Bind(options.Id.RawId)
                                           .SetPageSize(options.BatchSize)
                                           .SetAutoPage(false);

            while (pagingInfo.HasMore)
            {
                if (pagingInfo.HasToken())
                    bs.SetPagingState(pagingInfo.Token);

                RowSet rows = await session.ExecuteAsync(bs).ConfigureAwait(false);

                foreach (var row in rows)
                {
                    byte[] data = row.GetValue<byte[]>(ProjectionColumn.EventData);
                    if (data is not null)
                    {
                        IEvent @event = serializer.DeserializeFromBytes<IEvent>(data);
                        events.Add(@event);
                    }
                    else
                    {
                        LogError(logger, options.Version.ProjectionName, Convert.ToBase64String(options.Id.RawId.Span), null);
                    }
                }
                pagingInfo = PagingInfo.From(rows);
            }

            return new ProjectionStream(options.Version, options.Id, events);
        }

        async Task<PreparedStatement> BuildInsertPreparedStatementAsync(string columnFamily, ISession session)
        {
            string key = $"{session.Keyspace}_{columnFamily}";

            if (SavePreparedStatementsLegacy.TryGetValue(key, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(InsertQueryTemplate, session.Keyspace, columnFamily));
                statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                SavePreparedStatementsLegacy.TryAdd(key, statement);
            }
            return statement;
        }

        async Task<PreparedStatement> GetPreparedStatementToGetProjectionAsync(string columnFamily, ISession session)
        {
            string key = $"{session.Keyspace}_{columnFamily}";

            if (GetPreparedStatementsLegacy.TryGetValue(key, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(GetQueryTemplate, session.Keyspace, columnFamily)).ConfigureAwait(false);
                statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                GetPreparedStatementsLegacy.TryAdd(key, statement);
            }
            return statement;
        }

        async Task<PreparedStatement> GetAsOfDatePreparedStatementAsync(string columnFamily, ISession session)
        {
            string key = $"{session.Keyspace}_{columnFamily}";

            if (GetAsOfDatePreparedStatementsLegacy.TryGetValue(key, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(GetQueryAsOfTemplate, session.Keyspace, columnFamily)).ConfigureAwait(false);
                statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                GetAsOfDatePreparedStatementsLegacy.TryAdd(key, statement);
            }
            return statement;
        }

        async Task<PreparedStatement> GetDescendingPreparedStatementAsync(string columnFamily, ISession session)
        {
            string key = $"{session.Keyspace}_{columnFamily}";

            if (GetDescendingPreparedStatementsLegacy.TryGetValue(key, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(GetQueryDescendingTemplate, session.Keyspace, columnFamily)).ConfigureAwait(false);
                statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                GetDescendingPreparedStatementsLegacy.TryAdd(key, statement);
            }
            return statement;
        }

        private async Task<PreparedStatement> GetWritePartitionsPreparedStatementAsync(ISession session)
        {
            if (InsertPartitionPreparedStatements.TryGetValue(session.Keyspace, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(InsertPartition, session.Keyspace)).ConfigureAwait(false);
                statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                InsertPartitionPreparedStatements.TryAdd(session.Keyspace, statement);
            }
            return statement;
        }

        private async Task<PreparedStatement> BuildNewInsertPreparedStatementAsync(string columnFamily, ISession session)
        {
            string key = $"{session.Keyspace}_{columnFamily}";

            if (SavePreparedStatementsNew.TryGetValue(key, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(InsertNewQueryTemplate, session.Keyspace, columnFamily));
                statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                SavePreparedStatementsNew.TryAdd(key, statement);
            }
            return statement;
        }

        private static long CalculatePartition(IEvent @event) // TODO: This will be extended in future version to be configurable for every projection
        {
            int month = @event.Timestamp.Month;
            int partitionId = @event.Timestamp.Year * 100 + month;

            return partitionId;
        }
    }
}
