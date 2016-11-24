//using System.Collections.Generic;
//using System;
//using System.Runtime.InteropServices;
//using Elders.Cronus.DomainModeling;
//using Elders.Cronus.DomainModeling.Projections;

//namespace Elders.Cronus.Projections.Cassandra
//{
//    /// <summary>
//    /// Not thread safe
//    /// </summary>
//    public class Repository : IRepository
//    {
//        private readonly IPersiter persister;
//        private readonly Dictionary<string, Dictionary<string, Tuple<object, KeyValueData>>> track = new Dictionary<string, Dictionary<string, Tuple<object, KeyValueData>>>();
//        private readonly Dictionary<string, Dictionary<Type, List<Tuple<object, KeyValueCollectionItem>>>> collectionTrack = new Dictionary<string, Dictionary<Type, List<Tuple<object, KeyValueCollectionItem>>>>();

//        public Repository(IPersiter persister, Func<object, byte[]> serializer, Func<byte[], object> desirealizer)
//        {
//            this.persister = persister;
//            Serializer = serializer;
//            Desirealizer = desirealizer;
//        }

//        public Func<object, byte[]> Serializer { get; private set; }

//        public Func<byte[], object> Desirealizer { get; private set; }

//        public void ClearTrack()
//        {
//            track.Clear();
//            collectionTrack.Clear();
//        }

//        public void CommitChanges()
//        {
//            foreach (var items in track)
//            {
//                foreach (var item in items.Value)
//                {
//                    var after = Serializer(item.Value.Item1);
//                    if (!ByteArrayCompare(after, item.Value.Item2.Blob))
//                        persister.Update(item.Value.Item2, after);
//                }
//            }
//            foreach (var collections in collectionTrack)
//            {
//                foreach (var types in collections.Value)
//                {
//                    foreach (var item in types.Value)
//                    {
//                        var after = Serializer(item.Item1);
//                        if (!ByteArrayCompare(after, item.Item2.Blob))
//                            persister.Update(item.Item2, after);

//                    }
//                }
//            }
//            track.Clear();
//            collectionTrack.Clear();
//        }

//        public void Delete<T, V>(T obj) where T : IDataTransferObject<V>
//        {
//            persister.Delete(GetObjectId<T, V>(obj), typeof(T).GetColumnFamily());
//        }

//        public void Delete(object id, Type projectionStateType)
//        {
//            persister.Delete(ConvertIdToString(id), projectionStateType.GetColumnFamily());
//        }

//        public void DeleteCollectionItem<T, V, C>(T obj) where T : ICollectionDataTransferObjectItem<V, C>
//        {
//            persister.DeleteCollectionItem(new KeyValueCollectionItem(ConvertIdToString(obj.CollectionId), ConvertIdToString(obj.Id), typeof(T).GetColumnFamily(), null));
//        }

//        public void DeleteCollectionItem(object collectionIds, object itemId, Type projectionStateType)
//        {
//            persister.DeleteCollectionItem(new KeyValueCollectionItem(ConvertIdToString(collectionIds), ConvertIdToString(itemId), projectionStateType.GetColumnFamily(), null));
//        }

//        public T Get<T, V>(V ids) where T : IDataTransferObject<V>
//        {
//            var tableName = typeof(T).GetColumnFamily();
//            var id = ConvertIdToString(ids);
//            if (track.ContainsKey(tableName))
//            {
//                if (track[tableName].ContainsKey(id))
//                    return (T)track[tableName][id].Item1;
//            }
//            else
//            {
//                track.Add(tableName, new Dictionary<string, Tuple<object, KeyValueData>>());
//            }
//            var data = persister.Get(id, tableName);
//            if (data == null)
//                return default(T);
//            var item = (T)Desirealizer(data.Blob);
//            track[tableName].Add(id, new Tuple<object, KeyValueData>(item, data));
//            return item;
//        }

//        public T Get<T>(object ids)
//        {
//            var tableName = typeof(T).GetColumnFamily();
//            var id = ConvertIdToString(ids);
//            if (track.ContainsKey(tableName))
//            {
//                if (track[tableName].ContainsKey(id))
//                    return (T)track[tableName][id].Item1;
//            }
//            else
//            {
//                track.Add(tableName, new Dictionary<string, Tuple<object, KeyValueData>>());
//            }
//            var data = persister.Get(id, tableName);
//            if (data == null)
//                return default(T);
//            var item = (T)Desirealizer(data.Blob);
//            track[tableName].Add(id, new Tuple<object, KeyValueData>(item, data));
//            return item;
//        }

//        /// <summary>
//        /// Probably this should be called in the generic implementation. If you do not know what this is doing internaly and the reason to have this method please do not use it in your code.
//        /// </summary>
//        /// <param name="ids"></param>
//        /// <param name="projectionStateType"></param>
//        /// <returns></returns>
//        public object Get(object ids, Type projectionStateType)
//        {
//            var tableName = projectionStateType.GetColumnFamily();
//            var id = ConvertIdToString(ids);
//            if (track.ContainsKey(tableName))
//            {
//                if (track[tableName].ContainsKey(id))
//                    return track[tableName][id].Item1;
//            }
//            else
//            {
//                track.Add(tableName, new Dictionary<string, Tuple<object, KeyValueData>>());
//            }
//            var data = persister.Get(id, tableName);
//            if (data == null)
//                return null;
//            var item = Desirealizer(data.Blob);
//            track[tableName].Add(id, new Tuple<object, KeyValueData>(item, data));
//            return item;
//        }

//        public IEnumerable<T> GetAsCollectionItems<T, C>(C collectionIds) where T : ICollectionDataTransferObject<C>
//        {
//            var collectionId = ConvertIdToString(collectionIds);
//            if (!collectionTrack.ContainsKey(collectionId))
//                collectionTrack.Add(collectionId, new Dictionary<Type, List<Tuple<object, KeyValueCollectionItem>>>());
//            var typeofT = typeof(T);
//            if (collectionTrack[collectionId].ContainsKey(typeofT))
//            {
//                if (collectionTrack[collectionId][typeofT].Count > 0)
//                {
//                    foreach (var item in collectionTrack[collectionId][typeofT])
//                    {
//                        yield return (T)item.Item1;
//                    }
//                    yield break;
//                }
//            }
//            else
//                collectionTrack[collectionId].Add(typeofT, new List<Tuple<object, KeyValueCollectionItem>>());

//            foreach (KeyValueCollectionItem data in persister.GetCollection(collectionId, typeofT.GetColumnFamily()))
//            {
//                if (data == null)
//                    continue;
//                var item = (T)Desirealizer(data.Blob);
//                collectionTrack[collectionId][typeof(T)].Add(new Tuple<object, KeyValueCollectionItem>(item, data));
//                yield return item;
//            }
//        }

//        public IEnumerable<object> GetAsCollectionItems(object collectionIds, Type projectionStateType)
//        {
//            var collectionId = ConvertIdToString(collectionIds);
//            if (!collectionTrack.ContainsKey(collectionId))
//                collectionTrack.Add(collectionId, new Dictionary<Type, List<Tuple<object, KeyValueCollectionItem>>>());
//            var typeofT = projectionStateType;
//            if (collectionTrack[collectionId].ContainsKey(typeofT))
//            {
//                if (collectionTrack[collectionId][typeofT].Count > 0)
//                {
//                    foreach (var item in collectionTrack[collectionId][typeofT])
//                    {
//                        yield return item.Item1;
//                    }
//                    yield break;
//                }
//            }
//            else
//                collectionTrack[collectionId].Add(typeofT, new List<Tuple<object, KeyValueCollectionItem>>());

//            foreach (KeyValueCollectionItem data in persister.GetCollection(collectionId, typeofT.GetColumnFamily()))
//            {
//                if (data == null)
//                    continue;
//                var item = Desirealizer(data.Blob);
//                collectionTrack[collectionId][typeofT].Add(new Tuple<object, KeyValueCollectionItem>(item, data));
//                yield return item;
//            }
//        }

//        public object GetAsCollectionItem(object collectionId, object itemId, Type projectionStateType)
//        {
//            var collectionStringId = ConvertIdToString(collectionId);

//            var itemStringId = ConvertIdToString(itemId);

//            var typeofT = projectionStateType;

//            var result = persister.GetCollectionItem(collectionStringId, itemStringId, typeofT.GetColumnFamily());

//            if (result == null)
//                return null;

//            return Desirealizer(result.Blob);

//        }


//        public Query<T> Query<T>()
//        {
//            return new Query<T>(this);
//        }

//        public void Save<T, V>(T obj) where T : IDataTransferObject<V>
//        {
//            persister.Save(new KeyValueData(GetObjectId<T, V>(obj), obj.GetType().GetColumnFamily(), Serializer(obj)));
//        }

//        public void Save(object id, object obj)
//        {
//            persister.Save(new KeyValueData(ConvertIdToString(id), obj.GetType().GetColumnFamily(), Serializer(obj)));
//        }

//        public void Save<T, V, C>(params T[] items) where T : ICollectionDataTransferObjectItem<V, C>
//        {
//            foreach (T item in items)
//            {
//                persister.AddToCollection(new KeyValueCollectionItem(ConvertIdToString(item.CollectionId), ConvertIdToString(item.Id), typeof(T).GetColumnFamily(), Serializer(item)));
//            }
//        }

//        public void Save(object collectionId, object id, object obj)
//        {
//            persister.AddToCollection(new KeyValueCollectionItem(ConvertIdToString(collectionId), ConvertIdToString(id), obj.GetType().GetColumnFamily(), Serializer(obj)));
//        }

//        string GetObjectId<T, V>(T obj) where T : IDataTransferObject<V>
//        {
//            return ConvertIdToString(obj.Id);
//        }

//        string ConvertIdToString(object id)
//        {
//            if (id is string || id is Guid)
//                return id.ToString();

//            if (id is IBlobId)
//            {
//                return Convert.ToBase64String((id as IBlobId).RawId);
//            }
//            throw new NotImplementedException(String.Format("Unknow type id {0}", id.GetType()));
//        }

//        [DllImport("msvcrt.dll", CallingConvention = CallingConvention.Cdecl)]
//        static extern int memcmp(byte[] b1, byte[] b2, long count);
//        static bool ByteArrayCompare(byte[] b1, byte[] b2)
//        {
//            return b1.Length == b2.Length && memcmp(b1, b2, b1.Length) == 0;
//        }
//    }
//}
