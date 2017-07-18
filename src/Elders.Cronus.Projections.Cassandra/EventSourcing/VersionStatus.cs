using Elders.Cronus.DomainModeling;
using System;
using System.Runtime.Serialization;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    [DataContract(Name = "717ff3aa-7fd7-401d-aa0b-e7d970e25b13")]
    public class VersionStatus : ValueObject<VersionStatus>
    {
        VersionStatus() { }

        VersionStatus(string status)
        {
            this.status = status;
        }

        [DataMember(Order = 1)]
        string status;

        public static VersionStatus Building = new VersionStatus("building");

        public static VersionStatus Live = new VersionStatus("live");

        public static VersionStatus Create(string status)
        {
            switch (status?.ToLower())
            {
                case "building":
                    return Building;
                case "live":
                    return Live;
                default:
                    throw new NotSupportedException();
            }
        }

        public static implicit operator string(VersionStatus status)
        {
            if (ReferenceEquals(null, status) == true) throw new ArgumentNullException(nameof(status));
            return status.status;
        }


        public override string ToString()
        {
            return status;
        }
    }
}
