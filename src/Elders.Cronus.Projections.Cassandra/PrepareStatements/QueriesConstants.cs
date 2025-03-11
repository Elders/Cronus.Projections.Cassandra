namespace Elders.Cronus.Projections.Cassandra.PrepareStatements;

public static class QueriesConstants
{
    // Projection tables ----->
    public const string InsertTemplateQuery = @"INSERT INTO {0}.""{1}"" (id,pid,data,ts) VALUES (?,?,?,?);";
    public const string GetTemplateQuery = @"SELECT pid,data,ts FROM {0}.""{1}"" WHERE id=? AND pid=?;";
    public const string GetAsOfTemplateQuery = @"SELECT data,ts FROM {0}.""{1}"" WHERE id=? AND pid=? AND ts<=?;";
    public const string GetDescendingTemplateQuery = @"SELECT data,ts FROM {0}.""{1}"" WHERE id=? AND pid =? order by ts desc";

    // Partition table ---->
    public const string InsertPartition = @"INSERT INTO {0}.projection_partitions (pt,id,pid) VALUES (?,?,?);";

    public static class Legacy
    {
        // Projection tables --->
        public const string InsertQueryTemplate = @"INSERT INTO {0}.""{1}"" (id,data,ts) VALUES (?,?,?);";
        public const string GetQueryTemplate = @"SELECT data,ts FROM {0}.""{1}"" WHERE id=?;";
        public const string GetQueryAsOfTemplate = @"SELECT data,ts FROM {0}.""{1}"" WHERE id=? AND ts<=?;";
        public const string GetQueryDescendingTemplate = @"SELECT data,ts FROM {0}.""{1}"" WHERE id=? order by ts desc";
    }
}
