using Elders.Cronus.DomainModeling;
using System.Collections.Generic;
using System;
using Cassandra;
using System.Reflection;
using System.Linq;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public static class CasssandraExtensions
    {
        const string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, sm int, evarid text, evarrev int, evarts bigint, evarpos int, data blob, PRIMARY KEY ((id, sm), evarid, evarrev, evarpos, evarts)) WITH CLUSTERING ORDER BY (evarid ASC);";

        public static void InitializeProjectionDatabase(this ISession session, IEnumerable<Type> projections)
        {
            foreach (var projType in projections
                .Where(x => typeof(IProjectionDefinition).IsAssignableFrom(x))
                .Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IEventHandler<>))))
            {
                session.Execute(string.Format(CreateProjectionEventsTableTemplate, projType.GetColumnFamily()).ToLower());
            }
        }

        public static void InitializeProjectionDatabase(this ISession session, IEnumerable<Assembly> assemblyContainingProjections)
        {
            InitializeProjectionDatabase(session, assemblyContainingProjections.SelectMany(x => x.GetExportedTypes()));
        }

        public static string GetColumnFamily(this Type projectionType)
        {
            return projectionType.GetContractId().Replace("-", "").ToLower();
        }
    }
}
