using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Cassandra;
using Elders.Cronus.DomainModeling.Projections;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CasssandraCollectionPersister : IKeyValueCollectionPersister
    {
        private const string InsertQueryTemplate = @"INSERT INTO ""{0}"" (id,iid,data) VALUES (?,?,?);";

        private const string UpdateQueryTemplate = @"UPDATE ""{0}"" SET data = ? WHERE id=?  AND iid = ?;";

        private const string GetQueryTemplate = @"SELECT data,iid FROM ""{0}"" WHERE id = ?;";

        private const string GetItemQueryTemplate = @"SELECT data,iid FROM ""{0}"" WHERE id = ? AND iid = ?;";

        private const string DeleteQueryTemplate = @"DELETE FROM ""{0}"" WHERE id=? AND iid=?;";

        private readonly ConcurrentDictionary<string, PreparedStatement> SavePreparedStatements;

        private readonly ConcurrentDictionary<string, PreparedStatement> GetPreparedStatements;

        private readonly ConcurrentDictionary<string, PreparedStatement> GetItemPreparedStatements;

        private readonly ConcurrentDictionary<string, PreparedStatement> UpdatePreparedStatements;

        private readonly ConcurrentDictionary<string, PreparedStatement> DeletePreparedStatements;

        private readonly ConsistencyLevel writeConsistencyLevel;

        private readonly ConsistencyLevel readConsistencyLevel;

        private ISession session;

        public CasssandraCollectionPersister(ISession session, ConsistencyLevel writeConsistencyLevel, ConsistencyLevel readConsistencyLevel)
        {
            this.session = session;
            this.writeConsistencyLevel = writeConsistencyLevel;
            this.readConsistencyLevel = readConsistencyLevel;
            this.SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.GetItemPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.UpdatePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.DeletePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public IEnumerable<KeyValueCollectionItem> GetCollection(string collectionId, string columnFamily)
        {
            var statement = GetPreparedStatements.GetOrAdd(columnFamily, x => BuildeGetPreparedStatemnt(x));
            statement.SetConsistencyLevel(readConsistencyLevel);
            var result = session.Execute(statement.Bind(collectionId));
            foreach (var row in result)
            {
                yield return new KeyValueCollectionItem(collectionId, row.GetValue<string>("iid"), columnFamily, row.GetValue<byte[]>("data"));
            }
        }

        public KeyValueCollectionItem GetCollectionItem(string collectionId, string itemId, string columnFamily)
        {
            var statement = GetItemPreparedStatements.GetOrAdd(columnFamily, x => BuildeGetCollectionItemPreparedStatemnt(x));
            statement.SetConsistencyLevel(readConsistencyLevel);
            var result = session.Execute(statement.Bind(collectionId, itemId)).FirstOrDefault();
            if (result == null)
                return null;

            return new KeyValueCollectionItem(collectionId, result.GetValue<string>("iid"), columnFamily, result.GetValue<byte[]>("data"));
        }

        public void AddToCollection(KeyValueCollectionItem collectionItem)
        {
            var statement = SavePreparedStatements.GetOrAdd(collectionItem.Table, x => BuildeInsertPreparedStatemnt(x));
            statement.SetConsistencyLevel(writeConsistencyLevel);
            var result = session.Execute(statement.Bind(collectionItem.CollectionId, collectionItem.ItemId, collectionItem.Blob));
        }

        public void Update(KeyValueCollectionItem collectionItem, byte[] data)
        {
            var statement = UpdatePreparedStatements.GetOrAdd(collectionItem.Table, x => BuildeInsertPreparedStatemnt(x));
            statement.SetConsistencyLevel(writeConsistencyLevel);
            var result = session.Execute(statement.Bind(collectionItem.CollectionId, collectionItem.ItemId, data));
        }

        public void DeleteCollectionItem(KeyValueCollectionItem collectionItem)
        {
            var statement = DeletePreparedStatements.GetOrAdd(collectionItem.Table, x => BuildeDeletePreparedStatemnt(x));
            statement.SetConsistencyLevel(writeConsistencyLevel);
            var result = session.Execute(statement.Bind(collectionItem.CollectionId, collectionItem.ItemId));
        }

        private PreparedStatement BuildeGetPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(GetQueryTemplate, columnFamily));
        }

        private PreparedStatement BuildeGetCollectionItemPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(GetItemQueryTemplate, columnFamily));
        }

        private PreparedStatement BuildeInsertPreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(InsertQueryTemplate, columnFamily));
        }

        private PreparedStatement BuildeDeletePreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(DeleteQueryTemplate, columnFamily));
        }

        private PreparedStatement BuildeUpdatePreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(UpdateQueryTemplate, columnFamily));
        }
    }
}
