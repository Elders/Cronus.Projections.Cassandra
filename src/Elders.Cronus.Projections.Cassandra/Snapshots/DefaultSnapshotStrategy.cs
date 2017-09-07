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

        public int GetSnapshotMarker(IEnumerable<ProjectionCommit> commits, int lastSnapshotRevision)
        {
            var lastMarker = commits.Select(x => x.SnapshotMarker).DefaultIfEmpty(lastSnapshotRevision + 1).Max();

            var commitsWithMarker = commits.Where(x => x.SnapshotMarker == lastMarker);

            return commitsWithMarker.Count() >= eventsInSnapshot
                 ? lastMarker + 1
                 : lastMarker;
        }

        public IAmTheAnswerIfWeNeedToCreateSnapshot ShouldCreateSnapshot(IEnumerable<ProjectionCommit> commits, int lastSnapshotRevision)
        {
            var commitsAfterLastSnapshotRevision = commits.Where(x => x.SnapshotMarker > lastSnapshotRevision);
            int latestSnapshotMarker = commitsAfterLastSnapshotRevision.Select(x => x.SnapshotMarker).DefaultIfEmpty(lastSnapshotRevision + 1).Max();
            if (latestSnapshotMarker > lastSnapshotRevision)
            {
                bool shouldCreateSnapshot = commitsAfterLastSnapshotRevision.Count() >= eventsInSnapshot || commits.Select(x => x.TimeStamp).DefaultIfEmpty(DateTime.MaxValue).Min() <= DateTime.UtcNow - snapshotOffset;
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
