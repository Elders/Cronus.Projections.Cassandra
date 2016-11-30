using Elders.Cronus.DomainModeling;
using System;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IProjectionRegistryStore
    {
        void Save();
        void Get();
    }

    public class ProjectionRegistryStore : IProjectionRegistryStore
    {
        public void Get()
        {
            throw new NotImplementedException();
        }

        public void Save()
        {
            throw new NotImplementedException();
        }
    }

    public interface IProjectionRegistry
    {

    }

    public class DataContractProjectionRegistry : IProjectionRegistry
    {
        public void RegisterProjection(IProjection projection)
        {

        }
    }

    public interface IProjectionStore
    {
        ProjectionStream Load(Type projectionType, IBlobId projectionId, ISnapshot snapshot);
        ProjectionStream Load<T>(IBlobId projectionId, ISnapshot snapshot) where T : IProjectionDefinition;

        void Save(ProjectionCommit commit);
    }
}
