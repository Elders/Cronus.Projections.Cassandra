using Elders.Cronus.Projections.Cassandra.Config;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public static class ProjectionVersionExtensions
    {
        public static string GetColumnFamily(this ProjectionVersion version, string suffix = "")
        {
            var versionSuffix = string.Empty;
            if (ReferenceEquals(null, version) == false)
                versionSuffix = "_" + version.Hash;

            versionSuffix = versionSuffix + suffix;
            return version.ProjectionName.GetColumnFamily(versionSuffix);
        }

        public static string GetLiveColumnFamily(this ProjectionVersions versions)
        {
            ProjectionVersion liveVersion = versions.GetLive();
            return GetColumnFamily(liveVersion);
        }

        public static string GetSnapshotColumnFamily(this ProjectionVersion version, string suffix = "_sp")
        {
            return GetColumnFamily(version, suffix);
        }
    }
}
