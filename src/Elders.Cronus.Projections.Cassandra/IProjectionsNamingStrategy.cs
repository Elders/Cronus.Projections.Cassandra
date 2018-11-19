namespace Elders.Cronus.Projections.Cassandra
{
    public interface IProjectionsNamingStrategy
    {
        string GetColumnFamily(ProjectionVersion version, string preffix = "", string suffix = "");
        string GetSnapshotColumnFamily(ProjectionVersion version, string suffix = "_sp");
    }
}
