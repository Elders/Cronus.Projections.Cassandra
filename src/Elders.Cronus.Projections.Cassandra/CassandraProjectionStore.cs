﻿using System.Collections.Generic;
using System;
using Cassandra;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Logging;
using Elders.Cronus.Persistence.Cassandra;
using System.ComponentModel.DataAnnotations;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraProjectionStore<TSettings> : CassandraProjectionStore where TSettings : ICassandraProjectionStoreSettings
    {
        public CassandraProjectionStore(TSettings settings, ILogger<CassandraProjectionStore> logger) : base(settings.CassandraProvider, settings.Serializer, settings.ProjectionsNamingStrategy, logger) { }
    }

    public class CassandraProjectionStore : IProjectionStore
    {
        const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id,data,ts) VALUES (?,?,?);";
        const string GetQueryTemplate = @"SELECT data,ts FROM ""{0}"" WHERE id=?;";
        const string GetQueryAsOfTemplate = @"SELECT data,ts FROM ""{0}"" WHERE id=? AND ts<=?;";
        const string GetBeforeAndAfterQueryTemplate = @"SELECT data,ts FROM ""{0}"" WHERE id=? AND ts<=? AND ts>=?";

        private readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        private readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;
        private readonly ConcurrentDictionary<string, PreparedStatement> GetAsOfDatePreparedStatements;
        private readonly ConcurrentDictionary<string, PreparedStatement> GetBeforeAndAfterPreparedStatement;

        private readonly ICassandraProvider cassandraProvider;
        private readonly ISerializer serializer;
        private readonly VersionedProjectionsNaming naming;
        private readonly ILogger<CassandraProjectionStore> logger;

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

            SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetAsOfDatePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            GetBeforeAndAfterPreparedStatement = new ConcurrentDictionary<string, PreparedStatement>();
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
                    logger.Error(() => $"Failed to load event `data`");
                }
            }
        }

        public async Task EnumerateProjectionsAsync(ProjectionsOperator @operator, ProjectionQueryOptions options)
        {
            if (options.AsOf.HasValue)
            {
                var timestamp = TimestampOptions.GetAsOfOptions(options.AsOf.Value);
                var projectionTimestampOptions = new ProjectionQueryOptions(options.Id, options.Version, timestamp); // remove that when we remove asOf
                await EnumerateProjectionsAsOfDate(@operator, projectionTimestampOptions).ConfigureAwait(false);
            }
            else if (options.TimestampOptions is not null)
            {
                if (options.TimestampOptions.IsAsOfOption())
                {
                    await EnumerateProjectionsAsOfDate(@operator, options);
                }
                else if (options.TimestampOptions.IsOptionForPeriod())
                {
                    await EnumerateProjectionsForPeriod(@operator, options);
                }
            }
        }

        public Task SaveAsync(ProjectionCommit commit)
        {
            string projectionCommitLocationBasedOnVersion = naming.GetColumnFamily(commit.Version);
            return SaveAsync(commit, projectionCommitLocationBasedOnVersion);
        }

        async Task SaveAsync(ProjectionCommit commit, string columnFamily)
        {
            byte[] data = serializer.SerializeToBytes(commit.Event);

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await BuildInsertPreparedStatementAsync(columnFamily, session).ConfigureAwait(false);

            var result = await session.ExecuteAsync(statement
                .Bind(
                    commit.ProjectionId.RawId,
                    data,
                    commit.Event.Timestamp.ToFileTime()
                ))
                .ConfigureAwait(false);
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

        async Task EnumerateProjectionsForPeriod(ProjectionsOperator @operator, ProjectionQueryOptions options)
        {
            List<IEvent> events = new List<IEvent>();
            if (@operator.OnProjectionStreamLoadedAsync is not null)
            {
                await foreach (var @event in LoadForPeriodInternalAsync(options))
                {
                    events.Add(@event);
                }

                var stream = new ProjectionStream(options.Version, options.Id, events);
                await @operator.OnProjectionStreamLoadedAsync(@stream);
            }
        }

        async IAsyncEnumerable<IEvent> LoadAsOfDateInternalAsync(ProjectionQueryOptions options)
        {
            string columnFamily = naming.GetColumnFamily(options.Version);

            PagingInfo pagingInfo = new PagingInfo();
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement preparedStatement = await GetAsOfDatePreparedStatementAsync(columnFamily, session).ConfigureAwait(false);

            IStatement bs = preparedStatement.Bind(options.Id.RawId, options.TimestampOptions.Before.ToFileTime())
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
                        logger.Error(() => $"Failed to load event `data`");
                    }
                }
                pagingInfo = PagingInfo.From(rows);
            }
        }

        async IAsyncEnumerable<IEvent> LoadForPeriodInternalAsync(ProjectionQueryOptions options)
        {
            string columnFamily = naming.GetColumnFamily(options.Version);

            PagingInfo pagingInfo = new PagingInfo();
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement preparedStatement = await GetBeforeAndAfterPreparedStatementAsync(columnFamily, session).ConfigureAwait(false);

            IStatement bs = preparedStatement.Bind(options.Id.RawId, options.TimestampOptions.Before.ToFileTime(), options.TimestampOptions.After.ToFileTime())
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
                        logger.Error(() => $"Failed to load event `data`");
                    }
                }
                pagingInfo = PagingInfo.From(rows);
            }
        }

        async Task<PreparedStatement> BuildInsertPreparedStatementAsync(string columnFamily, ISession session)
        {
            if (!SavePreparedStatements.TryGetValue(columnFamily, out PreparedStatement statement))
            {
                statement = await session.PrepareAsync(string.Format(InsertQueryTemplate, columnFamily));
                statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                SavePreparedStatements.TryAdd(columnFamily, statement);
            }

            return statement;
        }

        async Task<PreparedStatement> GetPreparedStatementToGetProjectionAsync(string columnFamily, ISession session)
        {
            if (!GetPreparedStatements.TryGetValue(columnFamily, out PreparedStatement loadPreparedStatement))
            {
                loadPreparedStatement = await session.PrepareAsync(string.Format(GetQueryTemplate, columnFamily)).ConfigureAwait(false);
                loadPreparedStatement = loadPreparedStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                GetPreparedStatements.TryAdd(columnFamily, loadPreparedStatement);
            }
            return loadPreparedStatement;
        }

        async Task<PreparedStatement> GetAsOfDatePreparedStatementAsync(string columnFamily, ISession session)
        {
            if (GetAsOfDatePreparedStatements.TryGetValue(columnFamily, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(GetQueryAsOfTemplate, columnFamily)).ConfigureAwait(false);
                statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                GetAsOfDatePreparedStatements.TryAdd(columnFamily, statement);
            }
            return statement;
        }

        async Task<PreparedStatement> GetBeforeAndAfterPreparedStatementAsync(string columnFamily, ISession session)
        {
            if (GetBeforeAndAfterPreparedStatement.TryGetValue(columnFamily, out PreparedStatement statement) == false)
            {
                statement = await session.PrepareAsync(string.Format(GetBeforeAndAfterQueryTemplate, columnFamily)).ConfigureAwait(false);
                statement = statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                GetBeforeAndAfterPreparedStatement.TryAdd(columnFamily, statement);
            }
            return statement;
        }
    }
}
