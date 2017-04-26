namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IHaveState
    {
        object State { get; set; }
        void InitializeState(object state);
    }
}
