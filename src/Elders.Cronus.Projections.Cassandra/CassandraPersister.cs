using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Cassandra;
using Elders.Cronus.DomainModeling.Projections;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraPersister : IPersiter
    {
        private const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id,data) VALUES (?,?);";

        private const string UpdateQueryTemplate = @"UPDATE ""{0}"" SET data = ? WHERE id=?;";

        private const string GetKeyValueDataTemplate = @"SELECT data FROM ""{0}"" WHERE id = ?;";

        private const string DeleteQueryTemplate = @"DELETE FROM ""{0}"" WHERE id=?;";

        private readonly ISession session;

        private readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;

        private readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;

        private readonly ConcurrentDictionary<string, PreparedStatement> UpdatePreparedStatements;

        private readonly ConcurrentDictionary<string, PreparedStatement> DeletePreparedStatements;

        CasssandraCollectionPersister collPersister;

        public CassandraPersister(ISession session)
        {
            this.session = session;
            collPersister = new CasssandraCollectionPersister(session);
            this.SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.UpdatePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.DeletePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public void Save(KeyValueData data)
        {
            session.Execute(GetSavePreparedStatement(data.Table).Bind(data.ItemId, data.Blob));
        }

        private PreparedStatement GetSavePreparedStatement(string columnFamily)
        {
            return SavePreparedStatements.GetOrAdd(columnFamily, x => BuildPreparedStatementForKeyValueData(x));

        }

        private PreparedStatement BuildPreparedStatementForKeyValueData(string columnFamily)
        {
            return session.Prepare(String.Format(InsertQueryTemplate, columnFamily));
        }

        private PreparedStatement BuildPreparedStatementForUpdateKeyValueData(string columnFamily)
        {
            return session.Prepare(String.Format(UpdateQueryTemplate, columnFamily));
        }

        private PreparedStatement BuildeDeletePreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(DeleteQueryTemplate, columnFamily));
        }

        public KeyValueData Get(string id, string columnFamiliy)
        {
            BoundStatement bs = GetPreparedStatementToLoadKeyValueData(columnFamiliy).Bind(id);
            using (var result = session.Execute(bs))
            {
                var fetchedResult = result.GetRows().SingleOrDefault();
                if (fetchedResult == null)
                    return null;
                else
                {
                    return new KeyValueData(id, columnFamiliy, fetchedResult.GetValue<byte[]>("data"));
                }
            }
        }

        PreparedStatement GetPreparedStatementToLoadKeyValueData(string columnFamily)
        {
            PreparedStatement loadAggregatePreparedStatement;
            if (!GetPreparedStatements.TryGetValue(columnFamily, out loadAggregatePreparedStatement))
            {
                loadAggregatePreparedStatement = session.Prepare(String.Format(GetKeyValueDataTemplate, columnFamily));
                GetPreparedStatements.TryAdd(columnFamily, loadAggregatePreparedStatement);
            }
            return loadAggregatePreparedStatement;
        }

        public void Delete(string id, string table)
        {
            var statement = DeletePreparedStatements.GetOrAdd(table, x => BuildeDeletePreparedStatemnt(x));
            var result = session.Execute(statement.Bind(id));
        }

        public IEnumerable<KeyValueCollectionItem> GetCollection(string collectionId, string columnFamily)
        {
            return collPersister.GetCollection(collectionId, columnFamily);
        }

        public KeyValueCollectionItem GetCollectionItem(string collectionId, string itemId, string columnFamily)
        {
            return collPersister.GetCollectionItem(collectionId, itemId, columnFamily);
        }

        public void AddToCollection(KeyValueCollectionItem collectionItem)
        {
            collPersister.AddToCollection(collectionItem);
        }

        public void DeleteCollectionItem(KeyValueCollectionItem collectionItem)
        {
            collPersister.DeleteCollectionItem(collectionItem);
        }

        public void Update(KeyValueCollectionItem collectionItem, byte[] data)
        {
            collPersister.Update(collectionItem, data);
        }

        public void Update(KeyValueData item, byte[] data)
        {
            var statemen = UpdatePreparedStatements.GetOrAdd(item.Table, x => BuildPreparedStatementForUpdateKeyValueData(x));
            session.Execute(statemen.Bind(data, item.ItemId));
        }
    }
}
