using Elders.Cronus.DomainModeling;
using System.Collections.Generic;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IProjectionStore
    {
        IEnumerable<ProjectionCommit> Load(IBlobId projectionId, int fromRevision);

        void Save(ProjectionCommit commit);
    }
}
