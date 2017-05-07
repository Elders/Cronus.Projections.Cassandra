namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IProjectionGetResult<out T>
    {
        bool Success { get; }

        T Projection { get; }
    }
}