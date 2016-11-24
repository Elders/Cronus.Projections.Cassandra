namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class ProjectionGetResult<T>
    {
        public ProjectionGetResult(bool success, T projection)
        {
            Success = success;
            Projection = projection;
        }

        public bool Success { get; private set; }

        public T Projection { get; private set; }

        public static ProjectionGetResult<T> NoResult = new ProjectionGetResult<T>(false, default(T));
    }
}
