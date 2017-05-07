using Elders.Cronus.Projections.Cassandra.Snapshots;
using System.Collections.Generic;
using System.Linq;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class ProjectionStream
    {
        IList<ProjectionCommit> commits;
        readonly ISnapshot snapshot;

        public ProjectionStream(IList<ProjectionCommit> commits, ISnapshot snapshot)
        {
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
