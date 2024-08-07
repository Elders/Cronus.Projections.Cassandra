using System.Collections.Generic;
using System;
using Cassandra;
using System.Threading.Tasks;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;
using Elders.Cronus.Persistence.Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.Projections.Cassandra;
using Elders.Cronus;
using System.ComponentModel;
using System.Threading;
using System.Collections.Concurrent;
using System.Linq;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStore<TSettings> : CassandraProjectionStore where TSettings : ICassandraProjectionStoreSettings
    {
        public CassandraProjectionStore(TSettings settings, ILogger<CassandraProjectionStore> logger) : base(settings.CassandraProvider, settings.Partititons, settings.Serializer, settings.ProjectionsNamingStrategy, logger) { }
    }

    public class CassandraProjectionStore : IProjectionStore
    {
        // Projection tables ----->
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

        public CassandraProjectionStore(ICassandraProvider cassandraProvider, IProjectionPartionsStore projectionPartionsStore, ISerializer serializer, VersionedProjectionsNaming naming, ILogger<CassandraProjectionStore> logger)
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

        private async Task<List<IEvent>> LoadProjectionPartitionAsync(IComparable<long> partitionId, ProjectionVersion version, IBlobId projectionId, int take, CancellationToken ct)
        {
            int bum = 0;//deleteme

            if (bum != 0)
            {
                throw new Exception("Ami sega?");//deleteme
            }

            List<IEvent> loadedEvents = new List<IEvent>();
            PagingInfo pagingInfo = new PagingInfo();

            string columnFamily = naming.GetColumnFamily(version);

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement preparedStatement = await GetPreparedStatementToGetProjectionByAscendingAsync(columnFamily, session).ConfigureAwait(false);

            IStatement bs = preparedStatement.Bind(projectionId.RawId, partitionId)
                                            .SetPageSize(take)
                                            .SetAutoPage(false);

            while (pagingInfo.HasMore && ct.IsCancellationRequested == false)
            {
                if (pagingInfo.HasToken())
                    bs.SetPagingState(pagingInfo.Token);

                RowSet rows = await session.ExecuteAsync(bs).ConfigureAwait(false);
                IEnumerable<IEvent> events = DeserialzeEvents(rows);

                loadedEvents.AddRange(events);
                pagingInfo = PagingInfo.From(rows);

                logger.LogInformation($"&&&&&&&&&&&&&&&& Izpulnih se za partition {partitionId} i projId {Convert.ToBase64String(projectionId.RawId)}"); //deleteme
                await Task.Delay(5000);//deleteme
            }
            if (ct.IsCancellationRequested)//deleteme
            {
                logger.LogInformation($"%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% CANCELLLLL  {partitionId} i projId {Convert.ToBase64String(projectionId.RawId)}");//deleteme
            }

            return loadedEvents;
        }

        private async Task<List<IEvent>> LoadProjectionPartitionAsOfDateAsync(IComparable<long> partitionId, ProjectionVersion version, IBlobId projectionId, int take, DateTimeOffset date, CancellationToken ct)
        {
            List<IEvent> loadedEvents = new List<IEvent>();
            PagingInfo pagingInfo = new PagingInfo();

            string columnFamily = naming.GetColumnFamily(version);

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement preparedStatement = await GetAsOfDatePreparedStatementAsync(columnFamily, session).ConfigureAwait(false);

            IStatement bs = preparedStatement.Bind(projectionId.RawId, partitionId, date.ToFileTime())
                                            .SetPageSize(take)
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
                await EnumerateWithOptionsAsync(@operator, options).ConfigureAwait(false);
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

        /// <summary>
        /// Use this method when you don't mind that the events inside the stream are unordered.
        /// </summary>
        /// <param name="options"></param>
        /// <returns>The entire projection stream with unordered events</returns>
        private async Task<ProjectionStream> EnumerateProjectionStreamAsync(ProjectionQueryOptions options)
        {
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            try
            {
                string columnFamily = naming.GetColumnFamily(options.Version);

                List<IComparable<long>> partitions = await projectionPartionsStore.GetPartitionsAsync(options.Version.ProjectionName, options.Id).ConfigureAwait(false);
                if (partitions.Count == 0)
                    return ProjectionStream.Empty();

                int mdp = Environment.ProcessorCount;

                List<Task<List<IEvent>>> loadingTasks = new List<Task<List<IEvent>>>();
                List<IEvent> eventsLoaded = new List<IEvent>();

                for (int i = 0; i < partitions.Count; i++)
                {
                    Task<List<IEvent>> loadingPartitionTask = LoadProjectionPartitionAsync(partitions[i], options.Version, options.Id, options.BatchSize, cancellationTokenSource.Token);
                    loadingTasks.Add(loadingPartitionTask);

                    if (loadingTasks.Count >= mdp || i == partitions.Count - 1)
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
                return stream;
            }
            catch
            {
                cancellationTokenSource.Cancel();
                throw;
            }
        }

        private async Task<ProjectionStream> EnumerateProjectionDescendingStreamAsync(ProjectionQueryOptions options)
        {
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            try
            {
                string columnFamily = naming.GetColumnFamily(options.Version);

                List<IComparable<long>> partitions = await projectionPartionsStore.GetPartitionsAsync(options.Version.ProjectionName, options.Id).ConfigureAwait(false);
                if (partitions.Count == 0)
                    return ProjectionStream.Empty();

                int mdp = Environment.ProcessorCount;

                List<Task<List<IEvent>>> loadingTasks = new List<Task<List<IEvent>>>();
                List<IEvent> eventsLoaded = new List<IEvent>();

                for (int i = partitions.Count - 1; i == 0; i--)
                {
                    Task<List<IEvent>> loadingPartitionTask = LoadProjectionPartitionAsync(partitions[i], options.Version, options.Id, options.BatchSize, cancellationTokenSource.Token);
                    loadingTasks.Add(loadingPartitionTask);

                    if (loadingTasks.Count >= mdp || i == partitions.Count - 1)
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
                return stream;
            }
            catch
            {
                cancellationTokenSource.Cancel();
                throw;
            }
        }

        async Task EnumerateProjectionsAsOfDate(ProjectionsOperator @operator, ProjectionQueryOptions options)
        {
            List<IEvent> events = new List<IEvent>();
            if (@operator.OnProjectionStreamLoadedAsync is not null)
            {
                var stream = await LoadAsOfDateInternalAsync(options).ConfigureAwait(false);

                await @operator.OnProjectionStreamLoadedAsync(stream);
            }
        }

        async Task EnumerateWithOptionsAsync(ProjectionsOperator @operator, ProjectionQueryOptions options)
        {
            PagingProjectionsResult result;
            if (@operator.OnProjectionStreamLoadedAsync is not null)
            {
                ProjectionStream stream = await EnumerateProjectionStreamAsync(options).ConfigureAwait(false);
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

        /// <summary>
        /// Load all the events as of the provided date. The events are UNORDERED.
        /// </summary>
        /// <param name="options"></param>
        /// <returns>The unordered events as of some point in time</returns>
        private async Task<ProjectionStream> LoadAsOfDateInternalAsync(ProjectionQueryOptions options)
        {
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            try
            {
                string columnFamily = naming.GetColumnFamily(options.Version);

                List<IComparable<long>> partitions = await projectionPartionsStore.GetPartitionsAsync(options.Version.ProjectionName, options.Id).ConfigureAwait(false);
                if (partitions.Count == 0)
                    return ProjectionStream.Empty();

                Dictionary<long, DateTimeOffset> partitionsDictionary = partitions.ToDictionary(key => (long)key, GetDateBasedOnPartition); //opa

                var partitionsThatWeAreInterestedIn = partitionsDictionary.Where(x => x.Value <= options.AsOf.Value).Select(x=>x.Key).ToList();
                if(partitionsThatWeAreInterestedIn.Count == 0)
                    return ProjectionStream.Empty();

                int mdp = Environment.ProcessorCount;

                List<Task<List<IEvent>>> loadingTasks = new List<Task<List<IEvent>>>();
                List<IEvent> eventsLoaded = new List<IEvent>();

                for (int i = 0; i < partitionsThatWeAreInterestedIn.Count; i++)
                {
                    Task<List<IEvent>> loadingPartitionTask = LoadProjectionPartitionAsOfDateAsync(partitionsThatWeAreInterestedIn[i], options.Version, options.Id, options.BatchSize, options.AsOf.Value, cancellationTokenSource.Token);
                    loadingTasks.Add(loadingPartitionTask);

                    if (loadingTasks.Count >= mdp || i == partitionsThatWeAreInterestedIn.Count - 1)
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
                return stream;
            }
            catch
            {
                cancellationTokenSource.Cancel();
                throw;
            }
        }

        /// <summary>
        /// Use this method when you care about the order of the events
        /// </summary>
        /// <param name="options"></param>
        /// <returns>The ordered events with given options</returns>
        async Task<PagingProjectionsResult> EnumerateWithPagingInternalAsync(ProjectionQueryOptions options)
        {
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            try
            {
                string columnFamily = naming.GetColumnFamily(options.Version);

                List<IComparable<long>> partitions = await projectionPartionsStore.GetPartitionsAsync(options.Version.ProjectionName, options.Id).ConfigureAwait(false);
                if (partitions.Count == 0)
                    return new PagingProjectionsResult();

                int mdp = Environment.ProcessorCount;

                List<Task<List<IEvent>>> loadingTasks = new List<Task<List<IEvent>>>();
                List<IEvent> eventsLoaded = new List<IEvent>();

                for (int i = 0; i < partitions.Count; i++)
                {
                    Task<List<IEvent>> loadingPartitionTask = LoadProjectionPartitionAsync(partitions[i], options.Version, options.Id, options.BatchSize, cancellationTokenSource.Token);
                    loadingTasks.Add(loadingPartitionTask);

                    if (loadingTasks.Count >= mdp || i == partitions.Count - 1)
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
                return new PagingProjectionsResult();
            }
            catch
            {
                cancellationTokenSource.Cancel();
                throw;
            }
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

        async Task<PreparedStatement> GetPreparedStatementToGetProjectionByAscendingAsync(string columnFamily, ISession session)
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
            //long partitionId = @event.Timestamp.Year * 10000 + month * 1000 + day * 10; // tova raboti za sega
            long pid = @event.Timestamp.Year * 1000 + day;
            return pid;
        }

        private static DateTimeOffset GetDateBasedOnPartition(IComparable<long> partition)
        {
            long year = (long)partition / 1000; // opa
            long dayOfYear = (long)partition % 1000; // opa

            DateTime date = new DateTime((int)year, 1, 1).AddDays(dayOfYear - 1); // becuase we start from the 1st jan and we need to substract 1 to not calculate wrong
            DateTimeOffset dateTimeOffset = new DateTimeOffset(date, TimeSpan.Zero);

            return dateTimeOffset;
        }


        public IAsyncEnumerable<ProjectionCommit> LoadAsync(ProjectionVersion version, IBlobId projectionId)
        {
            throw new NotImplementedException();
        }

        private async Task<List<IEvent>> TestOff(IComparable<long> partitionId, IBlobId projectionId, ProjectionVersion version, Order order, int take, CancellationToken ct)
        {
            List<IEvent> loadedEvents = new List<IEvent>();
            PagingInfo pagingInfo = new PagingInfo();

            string columnFamily = naming.GetColumnFamily(version);
            ISession session = await GetSessionAsync().ConfigureAwait(false);

            PreparedStatement preparedStatement;

            if (order.Equals(Order.Ascending))
            {
                preparedStatement = await GetPreparedStatementToGetProjectionByAscendingAsync(columnFamily, session).ConfigureAwait(false);
            }
            else
            {
                preparedStatement = await GetDescendingPreparedStatementAsync(columnFamily, session).ConfigureAwait(false);
            }

            IStatement bs = preparedStatement.Bind(projectionId.RawId, partitionId)
                                            .SetPageSize(take)
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
    }
}
