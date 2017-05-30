using System;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.Projections.Cassandra.EventSourcing;
using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.Projections.Cassandra.Snapshots;

namespace Elders.Cronus.Projections.Cassandra.Tests
{
    public class MemoryProjectionStore : IProjectionStore
    {
        private List<ProjectionCommit> commits;

        public MemoryProjectionStore()
        {
            commits = new List<ProjectionCommit>();
        }

        public IProjectionBuilder GetBuilder(Type projectionType)
        {
            throw new NotImplementedException();
        }

        public ProjectionStream Load(Type projectionType, IBlobId projectionId, ISnapshot snapshot)
        {
            return new ProjectionStream(
                commits.Where(x =>
                    x.ProjectionId == projectionId
                    && x.SnapshotMarker > snapshot.Revision).ToList(),
                snapshot);
        }

        public ProjectionStream Load<T>(IBlobId projectionId, ISnapshot snapshot) where T : IProjectionDefinition
        {
            return Load(typeof(T), projectionId, snapshot);
        }

        public void Save(ProjectionCommit commit)
        {
            commits.Add(commit);
        }
    }
}
