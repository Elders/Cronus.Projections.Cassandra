namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public class TableRetentionOptions
    {
        public bool DeleteOldProjectionTables { get; set; } = true;
        public uint NumberOfOldProjectionTablesToRetain { get; set; } = 2;
    }
}
