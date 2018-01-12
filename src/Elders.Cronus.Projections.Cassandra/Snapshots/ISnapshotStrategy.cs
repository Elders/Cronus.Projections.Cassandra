using System.Collections.Generic;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public interface ISnapshotStrategy
    {
        IAmTheAnswerIfWeNeedToCreateSnapshot ShouldCreateSnapshot(IEnumerable<ProjectionCommit> commits, int lastSnapshotRevision);
    }
}
