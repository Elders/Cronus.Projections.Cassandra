namespace Elders.Cronus.Projections.Cassandra
{
    public static class TableTemplates
    {
        public const string CreateSnapshopEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, rev int, data blob, PRIMARY KEY (id, rev)) WITH CLUSTERING ORDER BY (rev DESC);";
    }
}
