using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.Infrastructure;

namespace Elders.Cronus.Projections.Cassandra.PrepareStatements.New
{
    class InsertPreparedStatementNew : PreparedStatementCache
    {
        public InsertPreparedStatementNew(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
        { }
        internal override string GetQueryTemplate() => QueriesConstants.InsertTemplateQuery;
    }
}
