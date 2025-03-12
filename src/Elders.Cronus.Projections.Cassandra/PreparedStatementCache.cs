using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.Infrastructure;

namespace Elders.Cronus.Projections.Cassandra;

internal abstract class PreparedStatementCache
{
    private SemaphoreSlim threadGate = new SemaphoreSlim(1);
    private readonly ICassandraProvider cassandraProvider;
    private readonly ICronusContextAccessor context;
    private Dictionary<string, PreparedStatement> _tenantCache;

    public PreparedStatementCache(ICronusContextAccessor cronusContextAccessor, ICassandraProvider cassandraProvider)
    {
        _tenantCache = new Dictionary<string, PreparedStatement>();

        this.context = cronusContextAccessor ?? throw new ArgumentNullException(nameof(cronusContextAccessor));
        this.cassandraProvider = cassandraProvider ?? throw new ArgumentNullException(nameof(cassandraProvider));
    }

    internal abstract string GetQueryTemplate();

    internal async Task<PreparedStatement> PrepareStatementAsync(ISession session, string columnFamily)
    {
        try
        {
            PreparedStatement preparedStatement = default;
            string key = $"{context.CronusContext.Tenant}_{columnFamily}";
            if (_tenantCache.TryGetValue(key, out preparedStatement) == false)
            {
                await threadGate.WaitAsync(10000).ConfigureAwait(false);
                if (_tenantCache.TryGetValue(key, out preparedStatement))
                    return preparedStatement;

                string keyspace = cassandraProvider.GetKeyspace();
                string template = GetQueryTemplate();

                if (string.IsNullOrEmpty(keyspace)) throw new Exception($"Invalid keyspace while preparing query template: {template}");
                if (string.IsNullOrEmpty(columnFamily)) throw new Exception($"Invalid table name while preparing query template: {template}");

                string query = string.Format(template, keyspace, columnFamily);

                preparedStatement = await session.PrepareAsync(query).ConfigureAwait(false);
                SetPreparedStatementOptions(preparedStatement);

                _tenantCache.TryAdd(key, preparedStatement);
            }

            return preparedStatement;
        }
        catch (InvalidQueryException)
        {
            throw; // this is OK exception which is handled on a higher level.
        }
        catch (Exception ex)
        {
            throw new Exception($"Failed to prepare query statement for {this.GetType().Name}", ex);
        }
        finally
        {
            threadGate?.Release();
        }
    }

    internal virtual void SetPreparedStatementOptions(PreparedStatement statement)
    {
        statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
    }
}
