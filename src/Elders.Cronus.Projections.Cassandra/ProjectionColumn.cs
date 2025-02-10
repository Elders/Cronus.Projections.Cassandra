namespace Elders.Cronus.Projections.Cassandra
{
    internal static class ProjectionColumn
    {
        public const string EventData = "data";
        public const string EventAggregateId = "evarid";
        public const string EventAggregateRevision = "evarrev";
        public const string EventAggregatePosition = "evarpos";
        public const string Partition = "pid";
    }
}
