using System;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class VersionModel
    {
        public VersionModel(string key, int version, VersionStatus status)
        {
            Key = key;
            Version = version;
            Status = status;
        }

        public string Key { get; private set; }

        public int Version { get; private set; }

        public VersionStatus Status { get; private set; }

        public string GetNextVersionLocation()
        {
            string buildVersion = string.Empty;
            if (Status == VersionStatus.Live)
                buildVersion = $"{Key}_{Version + 1}";
            else if (Status == VersionStatus.Building)
                buildVersion = $"{Key}_{Version}";

            return buildVersion;
        }

        public string GetPreviousVersionLocation()
        {
            string obsolateVersion = string.Empty;
            if (Status == VersionStatus.Live)
                obsolateVersion = $"{Key}_{Version - 1}";
            else if (Status == VersionStatus.Building)
                obsolateVersion = $"{Key}_{Version - 2}";

            return obsolateVersion;
        }


        public string GetLiveVersionLocation()
        {
            string liveLocation = string.Empty;
            if (Status == VersionStatus.Live)
                liveLocation = $"{Key}_{Version}";
            else if (Status == VersionStatus.Building)
                liveLocation = $"{Key}_{Version - 1}";

            return liveLocation;
        }
    }
}
