namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface ISnapshot : IHaveState
    {
        int Revision { get; }
    }

    public class Snapshot : ISnapshot
    {
        public Snapshot(object state, int revision)
        {
            State = state;
            Revision = revision;
        }

        public object State { get; set; }
        public int Revision { get; set; }

        public void InitializeState(object state)
        {
            State = state;
        }
    }
}
