namespace Elders.Cronus.Projections.Cassandra
{
    public interface IProjectionsNamingStrategy
    {
        string GetColumnFamily(ProjectionVersion version);
        string GetSnapshotColumnFamily(ProjectionVersion version);
    }
}
