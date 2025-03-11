using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.Infrastructure;

namespace Elders.Cronus.Projections.Cassandra.PrepareStatements.Legacy
{
    class AsOfDatePreparedStatementLegacy : PreparedStatementCache
    {
        public AsOfDatePreparedStatementLegacy(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider)
        { }
        internal override string GetQueryTemplate() => QueriesConstants.Legacy.GetQueryAsOfTemplate;
    }
}
