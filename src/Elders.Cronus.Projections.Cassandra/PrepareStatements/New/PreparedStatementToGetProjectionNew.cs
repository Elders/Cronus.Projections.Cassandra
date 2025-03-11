using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.Infrastructure;

namespace Elders.Cronus.Projections.Cassandra.PrepareStatements.New;

class PreparedStatementToGetProjectionNew : PreparedStatementCache
{
    public PreparedStatementToGetProjectionNew(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
    { }
    internal override string GetQueryTemplate() => QueriesConstants.GetTemplateQuery;
}
