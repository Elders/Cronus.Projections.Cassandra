using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public static class CronusMessageExtensions
    {
        public static EventOrigin GetEventOrigin(this CronusMessage message)
        {
            return new EventOrigin(GetRootId(message), GetRevision(message), GetRootEventPosition(message));
        }

        public static int GetRevision(this CronusMessage message)
        {
            var revision = 0;
            var value = string.Empty;
            if (message.Headers.TryGetValue(MessageHeader.AggregateRootRevision, out value) && int.TryParse(value, out revision))
                return revision;
            return 0;
        }

        public static int GetRootEventPosition(this CronusMessage message)
        {
            var revision = 0;
            var value = string.Empty;
            if (message.Headers.TryGetValue(MessageHeader.AggregateRootEventPosition, out value) && int.TryParse(value, out revision))
                return revision;
            return 0;
        }

        public static IAggregateRootId GetRootId(this CronusMessage message)
        {
            return null;
            //string value = null;
            //message.Headers.TryGetValue(MessageHeader.AggregateRootRevision, out value);
            //return value;
        }
    }
}
