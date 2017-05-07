using Elders.Cronus.DomainModeling;
using Elders.Cronus.Projections.Cassandra.Snapshots;
using System;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IProjectionStore
    {
        ProjectionStream Load(Type projectionType, IBlobId projectionId, ISnapshot snapshot);

        ProjectionStream Load<T>(IBlobId projectionId, ISnapshot snapshot) where T : IProjectionDefinition;

        void Save(ProjectionCommit commit);
    }
}
