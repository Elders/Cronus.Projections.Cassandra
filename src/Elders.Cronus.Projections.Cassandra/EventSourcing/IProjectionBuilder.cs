namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IProjectionBuilder
    {
        void Begin();

        void Populate(ProjectionCommit commit);

        void End();
    }
}
