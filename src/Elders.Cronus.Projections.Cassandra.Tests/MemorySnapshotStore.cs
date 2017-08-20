using System;
using Elders.Cronus.DomainModeling;
using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.Projections.Cassandra.Snapshots;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Elders.Cronus.Projections.Cassandra.Tests
{
    public class MemorySnapshotStore : ISnapshotStore
    {
        private List<ISnapshot> snapshots;

        public MemorySnapshotStore()
        {
            snapshots = new List<ISnapshot>();
        }

        public ISnapshot Load(string projectionContractId, IBlobId id)
        {
            var snapshot = snapshots
                .Where(x => x.Id.Equals(id))
                .OrderByDescending(x => x.Revision)
                .FirstOrDefault();

            if (ReferenceEquals(null, snapshot))
                return new NoSnapshot(id, projectionContractId);

            return new Snapshot(snapshot.Id, snapshot.ProjectionContractId, DeepClone(snapshot.State), snapshot.Revision);
        }

        public void Save(ISnapshot snapshot, bool isReplay)
        {
            snapshots.Add(new Snapshot(snapshot.Id, snapshot.ProjectionContractId, DeepClone(snapshot.State), snapshot.Revision));
        }

        private static T DeepClone<T>(T obj)
        {
            using (var ms = new MemoryStream())
            {
                var formatter = new BinaryFormatter();
                formatter.Serialize(ms, obj);
                ms.Position = 0;

                return (T)formatter.Deserialize(ms);
            }
        }
    }
}
