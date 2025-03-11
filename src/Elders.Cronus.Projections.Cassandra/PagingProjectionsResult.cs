using System.Collections.Generic;

namespace Elders.Cronus.Projections.Cassandra;

internal sealed class PagingProjectionsResult
{
    public PagingProjectionsResult()
    {
        Events = new List<IEvent>();
    }

    public PagingProjectionsResult(List<IEvent> events, byte[] newPagingToken)
    {
        Events = events;
        NewPagingToken = newPagingToken;
    }

    public List<IEvent> Events { get; set; }

    public byte[] NewPagingToken { get; set; }
}
