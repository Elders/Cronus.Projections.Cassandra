using Elders.Cronus.Projections.Cassandra.Snapshots;
using System.Collections.Generic;
using System.Linq;
using System;
using Elders.Cronus.DomainModeling.Projections;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class ProjectionStream
    {
        IList<ProjectionCommit> commits;
        readonly ISnapshot snapshot;

        public ProjectionStream(IList<ProjectionCommit> commits, ISnapshot snapshot)
        {
            if (ReferenceEquals(null, commits) == true) throw new ArgumentException(nameof(commits));
            if (ReferenceEquals(null, snapshot) == true) throw new ArgumentException(nameof(snapshot));

            this.snapshot = snapshot;
            this.commits = commits;
        }

        public IEnumerable<ProjectionCommit> Commits { get { return commits.ToList().AsReadOnly(); } }

        public IProjectionGetResult<T> RestoreFromHistory<T>() where T : IProjectionDefinition
        {
            var events = commits.Select(x => x.Event).ToList();
            if (events.Count > 0)
            {
                var projection = (T)FastActivator.CreateInstance(typeof(T), true);

                if (ReferenceEquals(null, snapshot.State) == false)
                    projection.State = snapshot.State;

                projection.ReplayEvents(events);
                return new ProjectionGetResult<T>(true, projection);
            }
            else
            {
                return ProjectionGetResult<T>.NoResult;
            }
        }
    }
}
