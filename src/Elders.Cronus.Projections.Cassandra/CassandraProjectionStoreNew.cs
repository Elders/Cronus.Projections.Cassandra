using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.Persistence.Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Elders.Cronus.Projections.Cassandra;


public class CassandraProjectionStoreNew
{
    // projection tables ---->
    const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id,pid,data,ts) VALUES (?,?,?,?);";
    const string GetQueryTemplate = @"SELECT pid,data,ts FROM ""{0}"" WHERE id=? AND pid=?;";
    const string GetQueryAsOfTemplate = @"SELECT data,ts FROM ""{0}"" WHERE id=? AND pid=? AND ts<=?;";
    const string GetQueryDescendingTemplate = @"SELECT data,ts FROM ""{0}"" WHERE id=? AND pid =? order by ts desc";

    // partition tables ---->
    private const string InsertPartition = @"INSERT INTO projection_partitions (pt,id,pid) VALUES (?,?,?);";

    private readonly ICassandraProvider cassandraProvider;
    private readonly IProjectionPartionsStore projectionPartionsStore;
    private readonly ISerializer serializer;
    private readonly VersionedProjectionsNaming naming;
    private readonly ILogger<CassandraProjectionStore> logger;

    public static EventId CronusProjectionEventLoadError = new EventId(74300, "CronusProjectionEventLoadError");
    private static readonly Action<ILogger, string, string, Exception> LogError = LoggerMessage.Define<string, string>(LogLevel.Error, CronusProjectionEventLoadError, "Failed to load event data. Handler -> {cronus_projection_type} Projection id -> {cronus_projection_id}");

    private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

    public CassandraProjectionStoreNew(ICassandraProvider cassandraProvider, IProjectionPartionsStore projectionPartionsStore, ISerializer serializer, VersionedProjectionsNaming naming, ILogger<CassandraProjectionStore> logger)
    {
        if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
        if (serializer is null) throw new ArgumentNullException(nameof(serializer));
        if (naming is null) throw new ArgumentNullException(nameof(naming));

        this.cassandraProvider = cassandraProvider;
        this.projectionPartionsStore = projectionPartionsStore;
        this.serializer = serializer;
        this.naming = naming;
        this.logger = logger;
    }

    private async Task<List<IEvent>> LoadPartitionAsync(IComparable<long> partitionId, ProjectionQueryOptions options, CancellationToken ct)
    {
        List<IEvent> loadedEvents = new List<IEvent>();
        PagingInfo pagingInfo = new PagingInfo();

        string columnFamily = naming.GetColumnFamily(options.Version);

        ISession session = await GetSessionAsync().ConfigureAwait(false);
        PreparedStatement preparedStatement = await GetPreparedStatementToGetProjectionAsync(columnFamily, session).ConfigureAwait(false);

        IStatement bs = preparedStatement.Bind(options.Id.RawId, partitionId)
                                        .SetPageSize(options.BatchSize)
                                        .SetAutoPage(false);

        while (pagingInfo.HasMore && ct.IsCancellationRequested == false)
        {
            if (pagingInfo.HasToken())
                bs.SetPagingState(pagingInfo.Token);

            RowSet rows = await session.ExecuteAsync(bs).ConfigureAwait(false);
            IEnumerable<IEvent> events = DeserialzeEvents(rows);

            loadedEvents.AddRange(events);
            pagingInfo = PagingInfo.From(rows);
        }

        return loadedEvents;
    }

    private IEnumerable<IEvent> DeserialzeEvents(RowSet rows)
    {
        foreach (Row row in rows)
        {
            byte[] data = row.GetValue<byte[]>(ProjectionColumn.EventData);

            if (data is not null)
            {
                IEvent @event = serializer.DeserializeFromBytes<IEvent>(data);
                if (@event is not null)
                {
                    yield return @event;
                }
                else
                {
                    throw new Exception("Unable to load projection event data from storage.");
                }
            }
            else
            {
                throw new Exception("Unable to load projection event data from storage.");
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
        else
        {
            await EnumerateProjectionStreamAsync(@operator, options);
        }
    }

    public Task SaveAsync(ProjectionCommit commit)
    {
        string projectionCommitLocationBasedOnVersion = naming.GetColumnFamily(commit.Version);
        return SaveAsync(commit, projectionCommitLocationBasedOnVersion);
    }

    async Task SaveAsync(ProjectionCommit commit, string columnFamily)
    {
        ISession session = await GetSessionAsync().ConfigureAwait(false);

        BatchStatement batch = new BatchStatement();
        batch.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        batch.SetIdempotence(false);
        batch.SetBatchType(BatchType.Logged);

        long partitionId = CalculatePartition(commit.Event);

        byte[] data = serializer.SerializeToBytes(commit.Event);

        PreparedStatement projectionStatement = await BuildInsertPreparedStatementAsync(columnFamily, session).ConfigureAwait(false);
        BoundStatement projectionBoundStatement = projectionStatement.Bind(commit.ProjectionId.RawId, partitionId, data, partitionId);
        batch.Add(projectionBoundStatement);

        PreparedStatement partitionStatement = await GetWritePartitionsPreparedStatementAsync(session).ConfigureAwait(false);
        BoundStatement partitionBoundStatement = partitionStatement.Bind(commit.Version.ProjectionName, commit.ProjectionId.RawId, partitionId);
        batch.Add(partitionBoundStatement);

        await session.ExecuteAsync(batch).ConfigureAwait(false);
    }

    private async Task EnumerateProjectionStreamAsync(ProjectionsOperator @operator, ProjectionQueryOptions options)
    {
        if (@operator.OnProjectionStreamLoadedAsync is not null)
        {
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

            try
            {
                string columnFamily = naming.GetColumnFamily(options.Version);

                List<IComparable<long>> partitions = await projectionPartionsStore.GetPartitionsAsync(options.Version.ProjectionName, options.Id).ConfigureAwait(false);
                if (partitions.Count == 0)
                    return;

                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement preparedStatement = await GetPreparedStatementToGetProjectionAsync(columnFamily, session).ConfigureAwait(false);

                int mdp = Environment.ProcessorCount;

                List<Task<List<IEvent>>> loadingTasks = new List<Task<List<IEvent>>>();
                List<IEvent> eventsLoaded = new List<IEvent>();

                foreach (IComparable<long> partition in partitions)
                {
                    Task<List<IEvent>> loadingPartitionTask = LoadPartitionAsync(partition, options, cancellationTokenSource.Token);
                    loadingTasks.Add(loadingPartitionTask);

                    if (loadingTasks.Count >= mdp)
                    {
                        Task<List<IEvent>> completed = await Task.WhenAny(loadingTasks);
                        loadingTasks.Remove(completed);

                        if (completed.IsFaulted)
                        {
                            cancellationTokenSource.Cancel();
                        }

                        List<IEvent> result = await completed;
                        eventsLoaded.AddRange(result);
                    }
                }

                var tasksLeft = await Task.WhenAll(loadingTasks);
                foreach (List<IEvent> loadedEvents in tasksLeft)
                {
                    eventsLoaded.AddRange(loadedEvents);
                }

                ProjectionStream stream = new ProjectionStream(options.Version, options.Id, eventsLoaded);
                await @operator.OnProjectionStreamLoadedAsync(stream).ConfigureAwait(false);
            }
            catch
            {
                cancellationTokenSource.Cancel();
                throw;
            }
        }
    }


    private async Task<List<IEvent>> LoadProjectionEventsAsync(ProjectionQueryOptions option, IEnumerable<IComparable<long>> allPartitions)
    {
        CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        int mdp = Environment.ProcessorCount;

        List<Task<List<IEvent>>> loadingTasks = new List<Task<List<IEvent>>>();
        List<IEvent> eventsLoaded = new List<IEvent>();

        foreach (IComparable<long> partition in allPartitions)
        {
            Task<List<IEvent>> loadingPartitionTask = LoadPartitionAsync(partition, option, cancellationTokenSource.Token);
            loadingTasks.Add(loadingPartitionTask);

            if (loadingTasks.Count >= mdp)
            {
                Task<List<IEvent>> completed = await Task.WhenAny(loadingTasks);
                loadingTasks.Remove(completed);

                if (completed.IsCompletedSuccessfully)
                {
                    List<IEvent> result = await completed;
                    eventsLoaded.AddRange(result);
                }
                else
                {
                    logger.ErrorException(completed.Exception, () => $"Failed to load projection {Convert.ToBase64String(option.Id.RawId)}"); // better log
                }
            }
            cancellationTokenSource.Cancel();
        }

        var tasksLeft = await Task.WhenAll(loadingTasks);
        foreach (List<IEvent> loadedEvents in tasksLeft)
        {
            eventsLoaded.AddRange(loadedEvents);
        }

        return eventsLoaded;
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
        PagingProjectionsResult result;
        if (@operator.OnProjectionStreamLoadedAsync is not null)
        {
            result = await EnumerateWithPagingInternalAsync(options).ConfigureAwait(false);

            var stream = new ProjectionStream(options.Version, options.Id, result.Events);
            await @operator.OnProjectionStreamLoadedAsync(stream).ConfigureAwait(false);
        }
        else if (@operator.OnProjectionStreamLoadedWithPagingAsync is not null)
        {
            result = await EnumerateWithPagingInternalAsync(options).ConfigureAwait(false);

            var pagedStream = new ProjectionStream(options.Version, options.Id, result.Events);
            var pagedOptions = new PagingOptions(options.PagingOptions.Take, result.NewPagingToken, options.PagingOptions.Order);
            await @operator.OnProjectionStreamLoadedWithPagingAsync(pagedStream, pagedOptions).ConfigureAwait(false);
        }
    }

    async IAsyncEnumerable<IEvent> LoadAsOfDateInternalAsync(ProjectionQueryOptions options) // query naobratno
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
                    LogError(logger, options.Version.ProjectionName, Convert.ToBase64String(options.Id.RawId), null);
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
                LogError(logger, options.Version.ProjectionName, Convert.ToBase64String(options.Id.RawId), null);
            }
        }
        pagingResult.NewPagingToken = result.PagingState;
        return pagingResult;
    }

    private async Task<PreparedStatement> GetWritePartitionsPreparedStatementAsync(ISession session)
    {
        PreparedStatement writeStatement = await session.PrepareAsync(InsertPartition).ConfigureAwait(false);
        writeStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

        return writeStatement;
    }

    async Task<PreparedStatement> BuildInsertPreparedStatementAsync(string columnFamily, ISession session)
    {
        PreparedStatement statement = await session.PrepareAsync(string.Format(InsertQueryTemplate, columnFamily));
        statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

        return statement;
    }

    async Task<PreparedStatement> GetPreparedStatementToGetProjectionAsync(string columnFamily, ISession session)
    {
        PreparedStatement loadPreparedStatement = await session.PrepareAsync(string.Format(GetQueryTemplate, columnFamily)).ConfigureAwait(false);
        loadPreparedStatement = loadPreparedStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

        return loadPreparedStatement;
    }

    async Task<PreparedStatement> GetAsOfDatePreparedStatementAsync(string columnFamily, ISession session)
    {
        PreparedStatement statement = await session.PrepareAsync(string.Format(GetQueryAsOfTemplate, columnFamily)).ConfigureAwait(false);
        statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

        return statement;
    }

    async Task<PreparedStatement> GetDescendingPreparedStatementAsync(string columnFamily, ISession session)
    {
        PreparedStatement statement = await session.PrepareAsync(string.Format(GetQueryDescendingTemplate, columnFamily)).ConfigureAwait(false);
        statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

        return statement;
    }

    private static long CalculatePartition(IEvent @event) // nah
    {
        int month = @event.Timestamp.Month;
        int day = @event.Timestamp.DayOfYear;
        long partitionId = @event.Timestamp.Year * 10000 + month * 1000 + day * 10;

        return partitionId;
    }

    public IAsyncEnumerable<ProjectionCommit> LoadAsync(ProjectionVersion version, IBlobId projectionId)
    {
        throw new NotImplementedException();
    }
}

