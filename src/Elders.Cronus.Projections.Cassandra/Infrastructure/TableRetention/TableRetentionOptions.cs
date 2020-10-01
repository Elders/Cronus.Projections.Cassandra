namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public class TableRetentionOptions
    {
        public bool DeleteOldProjectionTables { get; set; } = false;// This is an experimental feature. It was created for Cosmos DB because M$ charges per table. Till then we will not enable it
        public uint NumberOfOldProjectionTablesToRetain { get; set; } = 2;
    }
}
