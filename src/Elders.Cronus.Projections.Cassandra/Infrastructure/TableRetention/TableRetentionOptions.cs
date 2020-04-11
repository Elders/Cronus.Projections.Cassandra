namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public class TableRetentionOptions
    {
        public bool DeleteOldProjectionTables { get; set; } = false;
        public uint NumberOfOldProjectionTablesToRetain { get; set; } = 2;
    }
}
