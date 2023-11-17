using System;

namespace Elders.Cronus.Projections.Cassandra
{
    public class VersionedProjectionsNaming
    {
        public string GetColumnFamily(ProjectionVersion version)
        {
            return $"{VersionPart(version)}";
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

        internal string NormalizeProjectionName(string projectionName)
        {
            return projectionName.Replace("-", "").ToLower();
        }

        string VersionPart(ProjectionVersion version)
        {
            return $"{NormalizeProjectionName(version.ProjectionName)}_{version.Revision}_{version.Hash}";
        }
    }
}
