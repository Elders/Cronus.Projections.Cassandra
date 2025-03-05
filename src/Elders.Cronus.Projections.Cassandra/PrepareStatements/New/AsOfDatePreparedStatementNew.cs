using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.Infrastructure;

namespace Elders.Cronus.Projections.Cassandra.PrepareStatements.New
{
    class AsOfDatePreparedStatementNew : PreparedStatementCache
    {
        public AsOfDatePreparedStatementNew(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
        { }
        internal override string GetQueryTemplate() => QueriesConstants.GetAsOfTemplateQuery;
    }
}
