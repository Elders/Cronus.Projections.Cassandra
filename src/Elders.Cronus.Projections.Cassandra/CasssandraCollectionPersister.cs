using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Cassandra;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.DomainModeling.Projections;

namespace Elders.Cronus.Projections.Cassandra
{
    public static class CasssandraCollectionPersisterExtensions
    {
        private const string CreateKeyValueDataColumnFamilyTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, data blob, PRIMARY KEY (id)) WITH compression = {{ 'sstable_compression' : '' }};";
        private const string CreateColumnFamilyTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, iid text, data blob, PRIMARY KEY (id,iid)) WITH compression = {{ 'sstable_compression' : '' }};";

        public static void InitializeProjectionDatabase(this ISession session, IEnumerable<Type> projectionTypes)
        {
            foreach (var projType in projectionTypes.Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(IDataTransferObject<>))))
            {
                session.Execute(string.Format(CreateKeyValueDataColumnFamilyTemplate, projType.GetColumnFamily()).ToLowerInvariant());
            }

            foreach (var projType in projectionTypes.Where(x => x.GetInterfaces().Any(y => y.IsGenericType && y.GetGenericTypeDefinition() == typeof(ICollectionDataTransferObjectItem<,>))))
            {
                session.Execute(string.Format(CreateColumnFamilyTemplate, projType.GetColumnFamily()).ToLowerInvariant());
            }
        }

        public static void InitializeProjectionDatabase(this ISession session, Assembly assemblyContainingProjections)
        {
            InitializeProjectionDatabase(session, assemblyContainingProjections.GetExportedTypes());
        }

        public static string GetColumnFamily(this Type projectionType)
        {
            return projectionType.GetContractId().Replace("-", "").ToLowerInvariant();
        }
    }

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

        private ISession session;

        public CasssandraCollectionPersister(ISession session)
        {
            this.session = session;
            this.SavePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.GetPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.GetItemPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.UpdatePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.DeletePreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        public IEnumerable<KeyValueCollectionItem> GetCollection(string collectionId, string columnFamily)
        {
            var statement = GetPreparedStatements.GetOrAdd(columnFamily, x => BuildeGetPreparedStatemnt(x));
            var result = session.Execute(statement.Bind(collectionId));
            foreach (var row in result)
            {
                yield return new KeyValueCollectionItem(collectionId, row.GetValue<string>("iid"), columnFamily, row.GetValue<byte[]>("data"));
            }
        }

        public KeyValueCollectionItem GetCollectionItem(string collectionId, string itemId, string columnFamily)
        {
            var statement = GetItemPreparedStatements.GetOrAdd(columnFamily, x => BuildeGetCollectionItemPreparedStatemnt(x));
            var result = session.Execute(statement.Bind(collectionId, itemId)).FirstOrDefault();
            if (result == null)
                return null;

            return new KeyValueCollectionItem(collectionId, result.GetValue<string>("iid"), columnFamily, result.GetValue<byte[]>("data"));
        }

        public void AddToCollection(KeyValueCollectionItem collectionItem)
        {
            var statement = SavePreparedStatements.GetOrAdd(collectionItem.Table, x => BuildeInsertPreparedStatemnt(x));
            var result = session.Execute(statement.Bind(collectionItem.CollectionId, collectionItem.ItemId, collectionItem.Blob));
        }

        public void DeleteCollectionItem(KeyValueCollectionItem collectionItem)
        {
            var statement = DeletePreparedStatements.GetOrAdd(collectionItem.Table, x => BuildeDeletePreparedStatemnt(x));
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

        public void Update(KeyValueCollectionItem collectionItem, byte[] data)
        {
            var statement = UpdatePreparedStatements.GetOrAdd(collectionItem.Table, x => BuildeInsertPreparedStatemnt(x));
            var result = session.Execute(statement.Bind(collectionItem.CollectionId, collectionItem.ItemId, data));
        }

        private PreparedStatement BuildeUpdatePreparedStatemnt(string columnFamily)
        {
            return session.Prepare(string.Format(UpdateQueryTemplate, columnFamily));
        }
    }
}
