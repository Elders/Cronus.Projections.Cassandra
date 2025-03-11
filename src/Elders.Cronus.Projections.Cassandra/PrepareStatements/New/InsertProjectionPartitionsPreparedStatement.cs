using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.Infrastructure;

namespace Elders.Cronus.Projections.Cassandra.PrepareStatements.New
{
    class InsertProjectionPartitionsPreparedStatement : PreparedStatementCache
    {
        public InsertProjectionPartitionsPreparedStatement(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
        { }
        internal override string GetQueryTemplate() => QueriesConstants.InsertPartition;
    }
}
