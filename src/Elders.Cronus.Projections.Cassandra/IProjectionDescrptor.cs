namespace Elders.Cronus.Projections.Cassandra
{
    public interface IProjectionDescrptor
    {
        long GetPartition(IEvent @event);
    }
}
