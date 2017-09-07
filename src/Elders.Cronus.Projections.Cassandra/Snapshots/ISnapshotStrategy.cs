using Elders.Cronus.Projections.Cassandra.EventSourcing;
using System.Collections.Generic;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public interface ISnapshotStrategy
    {
        int GetSnapshotMarker(IEnumerable<ProjectionCommit> commits, int lastSnapshotRevision);
        IAmTheAnswerIfWeNeedToCreateSnapshot ShouldCreateSnapshot(IEnumerable<ProjectionCommit> commits, int lastSnapshotRevision);
    }
}
