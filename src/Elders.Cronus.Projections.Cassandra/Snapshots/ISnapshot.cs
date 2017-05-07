using Elders.Cronus.DomainModeling;
using System;

namespace Elders.Cronus.Projections.Cassandra.Snapshots
{
    public interface ISnapshot
    {
        IBlobId Id { get; }
        int Revision { get; }
        object State { get; }
        Type ProjectionType { get; }
    }
}
