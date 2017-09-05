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

        public IAmTheAnswerIfWeNeedToCreateSnapshot ShouldCreateSnapshot(IEnumerable<ProjectionCommit> commits, int lastSnapshotRevision)
        {
            int latestSnapshotMarker = commits.Select(x => x.SnapshotMarker).DefaultIfEmpty(0).Max();
            if (latestSnapshotMarker > lastSnapshotRevision)
            {
                bool shouldCreateSnapshot = commits.Count() >= eventsInSnapshot || commits.Min(x => x.TimeStamp) <= DateTime.UtcNow - snapshotOffset;
                if (shouldCreateSnapshot)
                    return new IAmTheAnswerIfWeNeedToCreateSnapshot(latestSnapshotMarker);
            }

            return IAmTheAnswerIfWeNeedToCreateSnapshot.AndInThisCaseTheAnswerIsNo;
        }
    }

    public class IAmTheAnswerIfWeNeedToCreateSnapshot
    {
        public IAmTheAnswerIfWeNeedToCreateSnapshot(int revision)
        {
            KeepTheNextSnapshotRevisionHere = revision;
        }

        public bool ShouldCreateSnapshot { get { return KeepTheNextSnapshotRevisionHere > 0; } }

        public int KeepTheNextSnapshotRevisionHere { get; private set; }

        public static IAmTheAnswerIfWeNeedToCreateSnapshot AndInThisCaseTheAnswerIsNo = new IAmTheAnswerIfWeNeedToCreateSnapshot(-1);
    }
}
