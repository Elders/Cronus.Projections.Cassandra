using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Cassandra;
using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public interface IProjectionRepository
    {
        ProjectionGetResult<T> Get<T>(IBlobId projectionId) where T : IProjectionDefinition;
    }

    public static class CasssandraCollectionPersisterExtensions
    {
        private const string CreateProjectionEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, rev int, evarid text, evarrev int, evts bigint,  data blob, PRIMARY KEY ((id,rev),evarid,evarrev,evts)) WITH CLUSTERING ORDER BY (evarid ASC);";

        public static void InitializeProjectionDatabase(this ISession session, IEnumerable<Type> projections)
        {
            foreach (var projType in projections.Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IEventHandler<>))))
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
