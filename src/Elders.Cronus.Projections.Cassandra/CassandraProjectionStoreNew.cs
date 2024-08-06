using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.Persistence.Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Elders.Cronus.Projections.Cassandra;

public class CassandraProjectionStoreNew<TSettings> : CassandraProjectionStore where TSettings : ICassandraProjectionStoreSettings
{
    public CassandraProjectionStoreNew(TSettings settings, ILogger<CassandraProjectionStore> logger) : base(settings.CassandraProvider, settings.Partititons, settings.Serializer, settings.ProjectionsNamingStrategy, logger) { }
}

public class CassandraProjectionStoreNew : IProjectionStore
{
    const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id,pid,data,ts) VALUES (?,?,?,?);";
    const string GetQueryTemplate = @"SELECT pid,data,ts FROM ""{0}"" WHERE id=? AND pid=?;";
    const string GetQueryAsOfTemplate = @"SELECT data,ts FROM ""{0}"" WHERE id=? AND pid=? AND ts<=?;";
    const string GetQueryDescendingTemplate = @"SELECT data,ts FROM ""{0}"" WHERE id=? AND pid =? order by ts desc";

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

    public async IAsyncEnumerable<ProjectionCommit> LoadAsync(ProjectionVersion version, IBlobId projectionId)
    {
        yield return default;
    }


    public async Task<> EnumerateProjectionPartitions(ProjectionsOperator @operator, ProjectionQueryOptions options)
    {
        string columnFamily = naming.GetColumnFamily(options.Version);

        List<IComparable<long>> partitions = await projectionPartionsStore.GetPartitionsAsync(options.Version.ProjectionName, options.Id).ConfigureAwait(false);

        ISession session = await GetSessionAsync().ConfigureAwait(false);
        PreparedStatement preparedStatement = await GetPreparedStatementToGetProjectionAsync(columnFamily, session).ConfigureAwait(false);

        int mdp = Environment.ProcessorCount;
        List<Task> loadingTasks = new List<Task>();

        foreach (IComparable<long> partition in partitions)
        {
            Task partitionLoadTask = LoadPartitionAsync(preparedStatement, options.Id, partition);
            loadingTasks.Add(partitionLoadTask);
            if (loadingTasks.Count == mdp)
            {
                Task<> completed = await Task.WhenAny(loadingTasks).ConfigureAwait(false);
                loadingTasks.Remove(completed);
                RowSet result = completed.Result;
                //if(completed.IsFaulted)
            }
        }


    }

    private Task LoadPartitionAsync(PreparedStatement preparedStatement, IBlobId projectionId, IComparable<long> partitionId)
    {

        return Task.CompletedTask;

    }

    private async Task EnumerateEventByEventAsync(RowSet rows, ProjectionsOperator @operator, ProjectionQueryOptions options)
    {
        foreach (Row row in rows)
        {
            byte[] data = row.GetValue<byte[]>(ProjectionColumn.EventData);

            if (data is not null)
            {
                IEvent @event = serializer.DeserializeFromBytes<IEvent>(data);
                await @operator.OnProjectionEventLoadedAsync(@event).ConfigureAwait(false);
            }
            else
            {
                LogError(logger, options.Version.ProjectionName, Convert.ToBase64String(options.Id.RawId), null);
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
    } // fix this sh

    public Task SaveAsync(ProjectionCommit commit)
    {
        string projectionCommitLocationBasedOnVersion = naming.GetColumnFamily(commit.Version);
        return SaveAsync(commit, projectionCommitLocationBasedOnVersion);
    }

    async Task SaveAsync(ProjectionCommit commit, string columnFamily)
    {
        //ISession session = await GetSessionAsync().ConfigureAwait(false);
        //PreparedStatement writeStatement = await GetWriteStatementAsync(session).ConfigureAwait(false);
        //BatchStatement batch = new BatchStatement();
        //batch.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        //batch.SetIdempotence(false);
        //batch.SetBatchType(BatchType.Unlogged);


        long partitionId = CalculatePartition(commit.Event);

        byte[] data = serializer.SerializeToBytes(commit.Event);

        ISession session = await GetSessionAsync().ConfigureAwait(false);
        PreparedStatement projectionStatement = await BuildInsertPreparedStatementAsync(columnFamily, session).ConfigureAwait(false);

        var appendProjectionTask = session.ExecuteAsync(projectionStatement
            .Bind(
                commit.ProjectionId.RawId,
                partitionId,
                data,
                commit.Event.Timestamp.ToFileTime()
            ));

        ProjectionPartition partition = new ProjectionPartition(commit.Version.ProjectionName, commit.ProjectionId.RawId, partitionId); //loggee
        Task appendPartitionTask = projectionPartionsStore.AppendAsync(partition);

        await Task.WhenAll(appendProjectionTask, appendPartitionTask);
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
}

