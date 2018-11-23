using System;
using Elders.Cronus.Projections.Versioning;

namespace Elders.Cronus.Projections.Cassandra
{
    public class VersionedProjectionsNaming : IProjectionsNamingStrategy
    {
        public string GetColumnFamily(ProjectionVersion version)
        {
            return $"{VersionPart(version)}";
        }

        public string GetSnapshotColumnFamily(ProjectionVersion version)
        {
            return $"{GetColumnFamily(version)}_sp";
        }

        string NormalizeProjectionName(string projectionName)
        {
            return projectionName.Replace("-", "").ToLower();
        }

        string VersionPart(ProjectionVersion version)
        {
            string thisProjectionShouldNotHaveRevision = typeof(ProjectionVersionsHandler).GetContractId();
            if (version.ProjectionName.Equals(thisProjectionShouldNotHaveRevision, StringComparison.OrdinalIgnoreCase))
                return $"{NormalizeProjectionName(version.ProjectionName)}_{version.Hash}";

            return $"{NormalizeProjectionName(version.ProjectionName)}_{version.Revision}_{version.Hash}";
        }
    }
}
