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

        public ProjectionVersion Parse(string columnFamily)
        {
            var parts = columnFamily.Split('_');
            if (parts.Length < 3)
                throw new ArgumentException($"Unable to parse '{nameof(ProjectionVersion)}' from '{columnFamily}'.", nameof(columnFamily));

            if (int.TryParse(parts[1], out var revision) == false)
                throw new ArgumentException($"Invalid projection revision '{parts[1]}'.", nameof(columnFamily));

            return new ProjectionVersion(parts[0], ProjectionStatus.Create("unknown"), revision, parts[2]);
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
