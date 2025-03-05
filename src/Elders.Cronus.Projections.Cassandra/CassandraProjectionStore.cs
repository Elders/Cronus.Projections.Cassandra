using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Persistence.Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Elders.Cronus.Projections.Cassandra.PrepareStatements;
using Elders.Cronus.Projections.Cassandra.PrepareStatements.Legacy;
using Elders.Cronus.Projections.Cassandra.PrepareStatements.New;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Projections.Cassandra
{
    [Obsolete("Will be removed in v12")]
    public interface IProjectionStoreLegacy
    {
        IAsyncEnumerable<ProjectionCommit> LoadAsync(ProjectionVersion version, IBlobId projectionId);

        Task EnumerateProjectionsAsync(ProjectionsOperator @operator, ProjectionQueryOptions options);

        Task SaveAsync(ProjectionCommit commit);
    }

    [Obsolete("Will be removed in v12")]
    public class CassandraProjectionStore<TSettings> : CassandraProjectionStore where TSettings : ICassandraProjectionStoreSettings
    {
        public CassandraProjectionStore(ICronusContextAccessor cronusContextAccessor, TSettings settings, IOptions<CassandraProviderOptions> options, ILogger<CassandraProjectionStore> logger) : base(cronusContextAccessor, settings.CassandraProvider, settings.Serializer, settings.ProjectionsNamingStrategy, options, logger) { }
    }

    /// We tried to use <see cref="ISession.PrepareAsync(string, string)"/> where we wanted to specify the keyspace (we use [cqlsh 6.2.0 | Cassandra 5.0.2 | CQL spec 3.4.7 | Native protocol v5] cassandra)
    /// it seems like the driver does not have YET support for protocol v5 (still in beta). In code the driver is using protocol v4 (which is preventing us from using the above mentioned method)
    /// https://datastax-oss.atlassian.net/jira/software/c/projects/CSHARP/issues/CSHARP-856 as of 01.23.25 this epic is still in todo.
    [Obsolete("Will be removed in v12")]
    public partial class CassandraProjectionStore : IProjectionStoreLegacy
    {
        private readonly ICassandraProvider cassandraProvider;
        private readonly ISerializer serializer;
        private readonly VersionedProjectionsNaming naming;
        private readonly IOptions<CassandraProviderOptions> options;
        private readonly ILogger<CassandraProjectionStore> logger;

        public static EventId CronusProjectionEventLoadError = new EventId(74300, "CronusProjectionEventLoadError");
        private static readonly Action<ILogger, string, string, Exception> LogError = LoggerMessage.Define<string, string>(LogLevel.Error, CronusProjectionEventLoadError, "Failed to load event data. Handler -> {cronus_projection_type} Projection id -> {cronus_projection_id}");

        private InsertPreparedStatementNew _insertPreparedStatement;

        private InsertPreparedStatementLegacy _insertPreparedStatementLegacy;
        private WritePreparedStatementLegacy _writePreparedStatementLegacy;
        private PreparedStatementToGetProjectionLegacy _preparedStatementToGetProjectionLegacy;
        private AsOfDatePreparedStatementLegacy _asOfDatePreparedStatementLegacy;
        private DescendingPreparedStatementLegacy _descendingPreparedStatementLegacy;

        private PreparedStatement _insertPartitionPreparedStatement; // the store is registered as tenant singleton and the table is hardcoded so there could only be one prepared statement per tenant

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public CassandraProjectionStore(ICronusContextAccessor cronusContextAccessor, ICassandraProvider cassandraProvider, ISerializer serializer, VersionedProjectionsNaming naming, IOptions<CassandraProviderOptions> options, ILogger<CassandraProjectionStore> logger)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            if (serializer is null) throw new ArgumentNullException(nameof(serializer));
            if (naming is null) throw new ArgumentNullException(nameof(naming));

            this.cassandraProvider = cassandraProvider;
            this.serializer = serializer;
            this.naming = naming;
            this.options = options;
            this.logger = logger;

            _insertPreparedStatement = new InsertPreparedStatementNew(cronusContextAccessor, cassandraProvider);

            _insertPreparedStatementLegacy = new InsertPreparedStatementLegacy(cronusContextAccessor, cassandraProvider);
            _writePreparedStatementLegacy = new WritePreparedStatementLegacy(cronusContextAccessor, cassandraProvider);
            _preparedStatementToGetProjectionLegacy = new PreparedStatementToGetProjectionLegacy(cronusContextAccessor, cassandraProvider);
            _asOfDatePreparedStatementLegacy = new AsOfDatePreparedStatementLegacy(cronusContextAccessor, cassandraProvider);
            _descendingPreparedStatementLegacy = new DescendingPreparedStatementLegacy(cronusContextAccessor, cassandraProvider);
        }

        public async IAsyncEnumerable<ProjectionCommit> LoadAsync(ProjectionVersion version, IBlobId projectionId)
        {
            string columnFamily = naming.GetColumnFamily(version);

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement preparedStatement = await _preparedStatementToGetProjectionLegacy.PrepareStatementAsync(session, columnFamily).ConfigureAwait(false);
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

            if (options.Value.SaveToNewProjectionsTablesOnly == false)
            {
                // old projections
                PreparedStatement projectionStatement = await _insertPreparedStatementLegacy.PrepareStatementAsync(session, projectionCommitLocationBasedOnVersionLEGACY).ConfigureAwait(false);
                BoundStatement projectionBoundStatement = projectionStatement.Bind(projectionId, data, commit.Event.Timestamp.ToFileTime());
                batch.Add(projectionBoundStatement);
            }

            // new projections
            PreparedStatement projectionStatementNew = await _insertPreparedStatement.PrepareStatementAsync(session, projectionCommitLocationBasedOnVersionNEW).ConfigureAwait(false);
            BoundStatement projectionBoundStatementNew = projectionStatementNew.Bind(projectionId, partitionId, data, commit.Event.Timestamp.ToFileTime());
            batch.Add(projectionBoundStatementNew);

            // partitions
            PreparedStatement partitionStatement = await _writePreparedStatementLegacy.PrepareStatementAsync(session, projectionCommitLocationBasedOnVersionLEGACY).ConfigureAwait(false);
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
            PreparedStatement preparedStatement = await _asOfDatePreparedStatementLegacy.PrepareStatementAsync(session, columnFamily).ConfigureAwait(false);

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
                preparedStatement = await _descendingPreparedStatementLegacy.PrepareStatementAsync(session, columnFamily).ConfigureAwait(false);
            }
            else
            {
                preparedStatement = await _preparedStatementToGetProjectionLegacy.PrepareStatementAsync(session, columnFamily).ConfigureAwait(false);
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
                preparedStatement = await _descendingPreparedStatementLegacy.PrepareStatementAsync(session, columnFamily).ConfigureAwait(false);
            else
                preparedStatement = await _preparedStatementToGetProjectionLegacy.PrepareStatementAsync(session, columnFamily).ConfigureAwait(false);

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
        private static long CalculatePartition(IEvent @event) // TODO: This will be extended in future version to be configurable for every projection
        {
            int month = @event.Timestamp.Month;
            int partitionId = @event.Timestamp.Year * 100 + month;

            return partitionId;
        }
    }
}
