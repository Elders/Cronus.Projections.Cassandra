using System;

namespace Elders.Cronus.Projections.Cassandra
{
    static class CasssandraExtensions
    {
        internal static string GetColumnFamily(this Type projectionType, string sufix = "")
        {
            return projectionType.GetContractId().Replace("-", "").ToLower() + sufix;
        }

        internal static string GetColumnFamily(this string contractId, string sufix = "")
        {
            return contractId.Replace("-", "").ToLower() + sufix;
        }

        internal static string GetColumnFamily(this string contractId, ProjectionVersion version)
        {
            return contractId.Replace("-", "").ToLower() + "_" + version.Hash + "_" + version.Revision;
        }
    }
}
