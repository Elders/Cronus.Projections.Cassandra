using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Elders.Cronus.Projections.Cassandra.PrepareStatements;

namespace Elders.Cronus.Projections.Cassandra.PrepareStatements.Legacy
{
    class DescendingPreparedStatementLegacy : PreparedStatementCache
    {
        public DescendingPreparedStatementLegacy(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
        { }
        internal override string GetQueryTemplate() => QueriesConstants.Legacy.GetQueryDescendingTemplate;
    }
}
