using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Persistence.Cassandra;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Elders.Cronus.Projections.Cassandra.PrepareStatements.New;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Projections.Cassandra
{
    public interface IProjectionStoreNew
    {
        IAsyncEnumerable<ProjectionCommit> LoadAsync(ProjectionVersion version, IBlobId projectionId);

        Task EnumerateProjectionsAsync(ProjectionsOperator @operator, ProjectionQueryOptions options);

        Task SaveAsync(ProjectionCommit commit);
    }

    public class CassandraProjectionStoreNew<TSettings> : CassandraProjectionStoreNew where TSettings : ICassandraProjectionStoreSettingsNew
    {
        public CassandraProjectionStoreNew(ICronusContextAccessor cronusContextAccessor, TSettings settings, ILogger<CassandraProjectionStoreNew> logger) : base(cronusContextAccessor, settings.CassandraProvider, settings.Partititons, settings.Serializer, settings.ProjectionsNamingStrategy, logger) { }
    }

    /// We tried to use <see cref="ISession.PrepareAsync(string, string)"/> where we wanted to specify the keyspace (we use [cqlsh 6.2.0 | Cassandra 5.0.2 | CQL spec 3.4.7 | Native protocol v5] cassandra)
    /// it seems like the driver does not have YET support for protocol v5 (still in beta). In code the driver is using protocol v4 (which is preventing us from using the above mentioned method)
    /// https://datastax-oss.atlassian.net/jira/software/c/projects/CSHARP/issues/CSHARP-856 as of 01.23.25 this epic is still in todo.
    public partial class CassandraProjectionStoreNew : IProjectionStoreNew
    {
        private const int AsOfBatchTasks = 2;

        private readonly ICassandraProvider cassandraProvider;
        private readonly IProjectionPartionsStore projectionPartionsStore;
        private readonly ISerializer serializer;
        private readonly VersionedProjectionsNaming naming;

        private InsertPreparedStatementNew _insertPreparedStatement;
        private WritePreparedStatementNew _writePreparedStatementNew;
        private PreparedStatementToGetProjectionNew _preparedStatementToGetProjectionNew;
        private AsOfDatePreparedStatementNew _asOfDatePreparedStatementNew;
        private DescendingPreparedStatementNew _descendingPreparedStatementNew;

        private PreparedStatement _insertPartitionPreparedStatement; // the store is registered as tenant singleton and the table is hardcoded so there could only be one prepared statement per tenant


        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public CassandraProjectionStoreNew(ICronusContextAccessor cronusContextAccessor, ICassandraProvider cassandraProvider, IProjectionPartionsStore projectionPartionsStore, ISerializer serializer, VersionedProjectionsNaming naming, ILogger<CassandraProjectionStoreNew> logger)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            if (serializer is null) throw new ArgumentNullException(nameof(serializer));
            if (naming is null) throw new ArgumentNullException(nameof(naming));

            this.cassandraProvider = cassandraProvider;
            this.projectionPartionsStore = projectionPartionsStore;
            this.serializer = serializer;
            this.naming = naming;

            _insertPreparedStatement = new InsertPreparedStatementNew(cronusContextAccessor, cassandraProvider);
            _writePreparedStatementNew = new WritePreparedStatementNew(cronusContextAccessor, cassandraProvider);
            _preparedStatementToGetProjectionNew = new PreparedStatementToGetProjectionNew(cronusContextAccessor, cassandraProvider);
            _asOfDatePreparedStatementNew = new AsOfDatePreparedStatementNew(cronusContextAccessor, cassandraProvider);
            _descendingPreparedStatementNew = new DescendingPreparedStatementNew(cronusContextAccessor, cassandraProvider);
        }

        [Obsolete("This method will be removed in v11. Don't use it, it is not good for performance")]
        public IAsyncEnumerable<ProjectionCommit> LoadAsync(ProjectionVersion version, IBlobId projectionId)
        {
            throw new NotImplementedException();
        }

        public Task SaveAsync(ProjectionCommit commit)
        {
            string projectionCommitLocationBasedOnVersion = naming.GetColumnFamilyNew(commit.Version);
            return SaveAsync(commit, projectionCommitLocationBasedOnVersion);
        }

        /// <summary>
        /// Enumerating the whole projection stream returns the events unordered
        /// Enumerating as of some point in time returns the ordered events projection stream
        /// Enumerating the paged projection stream returns the ordered events in the specified order
        /// </summary>
        /// <param name="operator"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public async Task EnumerateProjectionsAsync(ProjectionsOperator @operator, ProjectionQueryOptions options)
        {
            if (options.PagingOptions is not null)
            {
                await EnumerateWithOptionsAsync(@operator, options).ConfigureAwait(false);
            }
            else if (options.AsOf.HasValue)
            {
                await EnumerateProjectionsAsOfDate(@operator, options).ConfigureAwait(false);
            }
        }

        private async Task SaveAsync(ProjectionCommit commit, string columnFamily)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);

            BatchStatement batch = new BatchStatement();
            batch.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            batch.SetIdempotence(false);
            batch.SetBatchType(BatchType.Logged);

            long partitionId = CalculatePartition(commit.Event);
            byte[] data = serializer.SerializeToBytes(commit.Event);
            byte[] projectionId = commit.ProjectionId.RawId.ToArray(); // the Bind() method invokes the driver serializers for each value

            PreparedStatement projectionStatement = await _insertPreparedStatement.PrepareStatementAsync(session, columnFamily).ConfigureAwait(false);
            BoundStatement projectionBoundStatement = projectionStatement.Bind(projectionId, partitionId, data, commit.Event.Timestamp.ToFileTime());
            batch.Add(projectionBoundStatement);

            PreparedStatement partitionStatement = await _writePreparedStatementNew.PrepareStatementAsync(session, columnFamily).ConfigureAwait(false);
            BoundStatement partitionBoundStatement = partitionStatement.Bind(commit.Version.ProjectionName, projectionId, partitionId);
            batch.Add(partitionBoundStatement);

            await session.ExecuteAsync(batch).ConfigureAwait(false);
        }

        private async Task EnumerateWithOptionsAsync(ProjectionsOperator @operator, ProjectionQueryOptions options)
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

        private async Task EnumerateProjectionsAsOfDate(ProjectionsOperator @operator, ProjectionQueryOptions options)
        {
            List<IEvent> events = new List<IEvent>();
            if (@operator.OnProjectionStreamLoadedAsync is not null)
            {
                var stream = await EnumerateAsOfDateInternalAsync(options).ConfigureAwait(false);

                await @operator.OnProjectionStreamLoadedAsync(stream);
            }
        }

        /// <summary>
        /// The events are unordered
        /// </summary>
        /// <param name="options"></param>
        /// <returns>The entire projection stream and the events are unordered</returns>
        private async Task<ProjectionStream> EnumerateProjectionStreamAsync(ProjectionQueryOptions options)
        {
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
            try
            {
                List<IComparable<long>> partitions = await projectionPartionsStore.GetPartitionsAsync(options.Version.ProjectionName, options.Id).ConfigureAwait(false);
                if (partitions.Count == 0)
                    return ProjectionStream.Empty();

                int mdp = Environment.ProcessorCount;

                List<Task<List<IEvent>>> loadingTasks = new List<Task<List<IEvent>>>();
                List<IEvent> eventsLoaded = new List<IEvent>();

                for (int i = 0; i < partitions.Count; i++)
                {
                    Task<List<IEvent>> loadingPartitionTask = LoadEntireProjectionPartitionAsync(partitions[i], options.Version, options.Id, options.BatchSize, cancellationTokenSource.Token);
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

        /// <summary>
        /// Loads events sequentially
        /// If the provided token is null it means we start loading from the beginning. Otherwize we <see cref="ProjectionPaginationTokenFactory.Parse(byte[], out long, out byte[])"/> the token
        /// </summary>
        /// <param name="options">The token <see cref="ProjectionQueryOptions.PagingOptions.PaginationToken"/> is EXPECTED to be either null or ENCODED with <see cref="ProjectionPaginationTokenFactory.Construct(long, byte[])"/></param>
        /// <returns>
        /// The ordered events and the new paging token
        /// If there are NO MORE results left we return null for paging state. If we havent reached the end of loading , the token will be returned in the follwing format <see cref="ProjectionPaginationTokenFactory.Parse(byte[], out long, out byte[])"/>
        /// </returns>
        private async Task<PagingProjectionsResult> EnumerateWithPagingInternalAsync(ProjectionQueryOptions options)
        {
            PagingProjectionsResult result = new PagingProjectionsResult();

            string columnFamily = naming.GetColumnFamilyNew(options.Version);

            List<IComparable<long>> allPartitions = await projectionPartionsStore.GetPartitionsAsync(options.Version.ProjectionName, options.Id).ConfigureAwait(false);
            if (allPartitions.Count == 0)
                return result;

            IEnumerable<IComparable<long>> partitionsToStartLoadingFrom = allPartitions;
            byte[] cassandraPagingToken = null;

            if (options.PagingOptions.PaginationToken is not null)
            {
                var pagedToken = ProjectionPaginationTokenFactory.Parse(options.PagingOptions.PaginationToken);
                cassandraPagingToken = pagedToken.CassandraToken;

                if (options.PagingOptions.Order.Equals(Order.Ascending))
                    partitionsToStartLoadingFrom = allPartitions.Where(x => (long)x >= pagedToken.PartitionId); // example: e1p1 e2p2 e3p3 e4p4; loaded 1 and 2; partitions -> 3 and 4 in that order; (e1p1 means event 1 partition 1)
                else
                    partitionsToStartLoadingFrom = allPartitions.Where(x => (long)x <= pagedToken.PartitionId).OrderByDescending(x => x); // example: e1p1 e2p2 e3p3 e4p4; loaded 4 and 3; pid left -> 2 and 1 in that order;  
            }
            else
            {
                if (options.PagingOptions.Order.Equals(Order.Descending))
                    partitionsToStartLoadingFrom = allPartitions.OrderByDescending(x => x);
            }

            int leftToLoad = options.BatchSize;
            foreach (var currentPartition in partitionsToStartLoadingFrom)
            {
                var pagedLoadResult = await LoadProjectionPartitionWithPagingAsync(options.Version, options.PagingOptions.Order, currentPartition, options.Id, cassandraPagingToken, leftToLoad).ConfigureAwait(false);

                result.Events.AddRange(pagedLoadResult.Events);

                leftToLoad = leftToLoad - pagedLoadResult.Events.Count;
                cassandraPagingToken = pagedLoadResult.CassandraToken;

                if (leftToLoad == 0)
                {
                    result.NewPagingToken = ProjectionPaginationTokenFactory.Construct((long)currentPartition, pagedLoadResult.CassandraToken);
                    break;
                }
            }
            return result;
        }

        /// <summary>
        /// Load the event stream of events as of the provided date. The events are unordered.
        /// </summary>
        /// <param name="options"></param>
        /// <returns>The unordered events as of some point in time</returns>
        private async Task<ProjectionStream> EnumerateAsOfDateInternalAsync(ProjectionQueryOptions options)
        {
            string columnFamily = naming.GetColumnFamilyNew(options.Version);

            List<IComparable<long>> partitions = await projectionPartionsStore.GetPartitionsAsync(options.Version.ProjectionName, options.Id).ConfigureAwait(false);
            if (partitions.Count == 0)
                return ProjectionStream.Empty();

            List<Task<List<IEvent>>> loadingTasks = new List<Task<List<IEvent>>>();
            List<IEvent> eventsLoaded = new List<IEvent>();

            int skip = 0;

            while (true)
            {
                IEnumerable<IComparable<long>> partitionBatch = partitions.Skip(skip).Take(AsOfBatchTasks);
                int partitionBatchCount = partitionBatch.Count(); // this is okay because the underline collection here is a list

                if (partitionBatchCount == 0)
                    break;

                skip += partitionBatchCount;

                List<IEvent> batchResult = await LoadProjectionBatchAsOfAsync(partitionBatch, options).ConfigureAwait(false);

                IEnumerable<IEvent> eventsPastTheAsOfDate = eventsLoaded.Where(x => x.Timestamp > options.AsOf.Value);
                if (eventsPastTheAsOfDate.Any())
                {
                    var applicableEvents = eventsLoaded.Where(x => x.Timestamp <= options.AsOf.Value);
                    eventsLoaded.AddRange(applicableEvents);

                    break;
                }
                else
                {
                    eventsLoaded.AddRange(batchResult);
                }
            }

            ProjectionStream stream = new ProjectionStream(options.Version, options.Id, eventsLoaded);
            return stream;
        }

        private async Task<List<IEvent>> LoadProjectionBatchAsOfAsync(IEnumerable<IComparable<long>> partitions, ProjectionQueryOptions options)
        {
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

            try
            {
                List<Task<List<IEvent>>> loadingTasks = new List<Task<List<IEvent>>>();
                List<IEvent> eventsLoaded = new List<IEvent>();

                foreach (var partition in partitions)
                {
                    Task<List<IEvent>> loadingPartitionTask = LoadProjectionPartitionAsOfDateAsync(partition, options.Version, options.Id, options.BatchSize, options.AsOf.Value, cancellationTokenSource.Token);
                    loadingTasks.Add(loadingPartitionTask);
                }
                Task<List<IEvent>> completed = await Task.WhenAny(loadingTasks);
                loadingTasks.Remove(completed);

                if (completed.IsFaulted)
                    cancellationTokenSource.Cancel();

                List<IEvent> result = await completed;
                eventsLoaded.AddRange(result);

                var tasksLeft = await Task.WhenAll(loadingTasks);
                foreach (List<IEvent> loadedEvents in tasksLeft)
                {
                    eventsLoaded.AddRange(loadedEvents);
                }

                return eventsLoaded;
            }
            catch
            {
                cancellationTokenSource.Cancel();
                throw;
            }
        }

        private async Task<List<IEvent>> LoadEntireProjectionPartitionAsync(IComparable<long> partitionId, ProjectionVersion version, IBlobId projectionId, int bacthTake, CancellationToken ct)
        {
            List<IEvent> loadedEvents = new List<IEvent>();
            PagingInfo pagingInfo = new PagingInfo();
            string columnFamily = naming.GetColumnFamilyNew(version);

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement preparedStatement = await _preparedStatementToGetProjectionNew.PrepareStatementAsync(session, columnFamily).ConfigureAwait(false);

            IStatement bs = preparedStatement.Bind(projectionId.RawId, partitionId)
                                            .SetPageSize(bacthTake)
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

        private async Task<(List<IEvent> Events, byte[] CassandraToken)> LoadProjectionPartitionWithPagingAsync(ProjectionVersion version, Order order, IComparable<long> partitionId, IBlobId projectionId, byte[] cassandraPagingToken, int toTake)
        {
            PagingInfo pagingInfo = new PagingInfo() { Token = cassandraPagingToken };

            List<IEvent> loadedEvents = new List<IEvent>();
            string columnFamily = naming.GetColumnFamilyNew(version);

            ISession session = await GetSessionAsync().ConfigureAwait(false);

            PreparedStatement preparedStatement;

            if (order.Equals(Order.Ascending))
                preparedStatement = await _preparedStatementToGetProjectionNew.PrepareStatementAsync(session, columnFamily).ConfigureAwait(false);
            else
                preparedStatement = await _descendingPreparedStatementNew.PrepareStatementAsync(session, columnFamily).ConfigureAwait(false);

            IStatement bs = preparedStatement.Bind(projectionId.RawId, partitionId)
                                            .SetPageSize(toTake)
                                            .SetAutoPage(false);

            while (pagingInfo.HasMore)
            {
                if (pagingInfo.Token is not null)
                    bs.SetPagingState(pagingInfo.Token);

                RowSet rows = await session.ExecuteAsync(bs).ConfigureAwait(false);

                IEnumerable<IEvent> events = DeserialzeEvents(rows);
                loadedEvents.AddRange(events);

                int loadedSoFar = loadedEvents.Count;
                pagingInfo = PagingInfo.From(rows);

                if (loadedSoFar == toTake)
                    break;
            }

            return (loadedEvents, pagingInfo.Token);
        }

        private async Task<List<IEvent>> LoadProjectionPartitionAsOfDateAsync(IComparable<long> partitionId, ProjectionVersion version, IBlobId projectionId, int take, DateTimeOffset date, CancellationToken ct)
        {
            List<IEvent> loadedEvents = new List<IEvent>();
            PagingInfo pagingInfo = new PagingInfo();

            string columnFamily = naming.GetColumnFamilyNew(version);

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement preparedStatement = await _asOfDatePreparedStatementNew.PrepareStatementAsync(session, naming.GetColumnFamilyNew(version)).ConfigureAwait(false);

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

        private static long CalculatePartition(IEvent @event) // TODO: This will be extended in future version to be configurable for every projection
        {
            int month = @event.Timestamp.Month;
            int partitionId = @event.Timestamp.Year * 100 + month;

            return partitionId;
        }
    }
}
