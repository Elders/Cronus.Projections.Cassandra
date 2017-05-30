using System;
using System.Collections.Concurrent;
using System.Linq;
using Cassandra;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class CassandraVersionStore : IVersionStore
    {
        const string VersionTableName = "versions";
        const string CreatVersionTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text PRIMARY KEY, version int, status text);";
        const string GetQueryTemplate = @"SELECT version, status FROM ""{0}"" WHERE id=?";
        const string UpdateQueryTemplate = @"UPDATE ""{0}"" SET version = ?, status = ? WHERE id=?;";

        readonly ISession session;
        readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;
        readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;

        public CassandraVersionStore(ISession session)
        {
            if (ReferenceEquals(null, session) == true) throw new ArgumentNullException(nameof(session));
            this.session = session;
            this.SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();

            session.Execute(string.Format(CreatVersionTableTemplate, VersionTableName));
        }

        public VersionModel Get(string key)
        {
            BoundStatement bs = GetPreparedStatementToGetVersion(VersionTableName).Bind(key);
            var result = session.Execute(bs);
            var row = result.GetRows().FirstOrDefault();

            VersionModel response = new VersionModel(key, 1, VersionStatus.Live);

            if (row != null)
            {
                var version = row.GetValue<int>("version");
                var status = row.GetValue<string>("status");
                response = new VersionModel(key, version, VersionStatus.Create(status));
            }

            return response;
        }

        public void Save(VersionModel model)
        {
            var statement = SavePreparedStatements.GetOrAdd(VersionTableName, x => BuildUpdatePreparedStatemnt(VersionTableName));

            var result = session.Execute(statement
                .Bind(
                    model.Version,
                    model.Status.ToString(),
                    model.Key
                ));
        }

        PreparedStatement GetPreparedStatementToGetVersion(string columnFamily)
        {
            PreparedStatement loadAggregatePreparedStatement;
            if (!GetPreparedStatements.TryGetValue(columnFamily, out loadAggregatePreparedStatement))
            {
                loadAggregatePreparedStatement = session.Prepare(string.Format(GetQueryTemplate, columnFamily));
                GetPreparedStatements.TryAdd(columnFamily, loadAggregatePreparedStatement);
            }
            return loadAggregatePreparedStatement;
        }

        PreparedStatement BuildUpdatePreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(UpdateQueryTemplate, columnFamily));
        }
    }
}
