using Newtonsoft.Json.Linq;
using System;
using System.Reflection;
using System.Runtime.InteropServices.Marshalling;

namespace Elders.Cronus.Projections.Cassandra
{
    public static class Neshto
    {

    }
    public class VersionedProjectionsNaming
    {
        private static readonly char Dash = '-';

        public ReadOnlySpan<char> GetColumnFamily(ProjectionVersion version) // for old projection tables
        {
            return $"{VersionPart(version)}";
        }

        public ReadOnlySpan<char> GetColumnFamilyNew(ProjectionVersion version) // for tables with new partitionId
        {
            return $"{VersionPart(version)}_new"; // v11
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

        //string NormalizeProjectionName(ReadOnlySpan<char> projectionName)
        //{
        //    Span<char> result = stackalloc char[projectionName.Length];

        //    int theIndex = 0;
        //    for (int i = 0; i < projectionName.Length; i++)
        //    {
        //        char character = projectionName[i];

        //        if (character.Equals(Dash))
        //            continue;

        //        if (char.IsUpper(character))
        //        {
        //            result[theIndex] = char.ToLower(character);
        //        }
        //        else
        //        {
        //            result[theIndex] = character;
        //        }
        //        theIndex++;
        //    }

        //    return result;
        //}

        string VersionPart(ProjectionVersion version)
        {
            string projectionName = version.ProjectionName;
            Span<char> result = stackalloc char[projectionName.Length];

            int theIndex = 0;
            for (int i = 0; i < projectionName.Length; i++)
            {
                char character = projectionName[i];

                if (character.Equals(Dash))
                    continue;

                if (char.IsUpper(character))
                {
                    result[theIndex] = char.ToLower(character);
                }
                else
                {
                    result[theIndex] = character;
                }
                theIndex++;
            }
            //ReadOnlySpan<char> normalizedName = NormalizeProjectionName(version.ProjectionName);
            ReadOnlySpan<char> constructed = $"{result}_{version.Revision}_{version.Hash}";

            return constructed.ToString();
        }
    }
}
