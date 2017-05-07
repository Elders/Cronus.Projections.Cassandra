using System;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.Projections.Cassandra.EventSourcing;
using Machine.Specifications;
using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Middleware;
using Elders.Cronus.Projections.Cassandra.Snapshots;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Elders.Cronus.Projections.Cassandra.Tests
{
    public class MemoryProjectionStore : IProjectionStore
    {
        private List<ProjectionCommit> commits;

        public MemoryProjectionStore()
        {
            commits = new List<ProjectionCommit>();
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