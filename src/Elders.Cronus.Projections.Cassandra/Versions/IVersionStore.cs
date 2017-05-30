namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IVersionStore
    {
        VersionModel Get(string key);

        void Save(VersionModel model);
    }
}
