namespace Elders.Cronus.Projections.Cassandra.Infrastructure
{
    public interface IProjectionTableRetentionStrategy
    {
        void Apply(ProjectionVersion currentProjectionVersion);
    }
}
