using Elders.Cronus.Projections.Cassandra.EventSourcing;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public class DefaultSnapshotStrategy : ISnapshotStrategy
    {
        private TimeSpan snapshotOffset;
        private int eventsInSnapshot;

        public DefaultSnapshotStrategy(TimeSpan snapshotOffset, int eventsInSnapshot)
        {
            this.snapshotOffset = snapshotOffset;
            this.eventsInSnapshot = eventsInSnapshot;
        }

        public int GetSnapshotMarker(IEnumerable<ProjectionCommit> commits)
        {
            if (commits.Any() == false)
            {
                return 1;
            }

            var lastMarker = commits.Max(x => x.SnapshotMarker);

            var commitsWithMarker = commits.Where(x => x.SnapshotMarker == lastMarker);

            return commitsWithMarker.Count() >= eventsInSnapshot
                 ? lastMarker + 1
                 : lastMarker;
        }

        public bool ShouldCreateSnapshot(IEnumerable<ProjectionCommit> commits)
        {
            if (commits.GroupBy(x => x.SnapshotMarker).Count() != 1)
            {
                throw new InvalidOperationException("Commits should have the same snapshot marker");
            }

            return commits.Count() >= eventsInSnapshot
                && commits.Min(x => x.TimeStamp) <= DateTime.UtcNow - snapshotOffset;
        }
    }
}
