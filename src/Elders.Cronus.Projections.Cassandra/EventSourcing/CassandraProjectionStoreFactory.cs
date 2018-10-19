using System.Collections.Generic;
using System;
using System.Linq;
using Elders.Cronus.Projections.Cassandra.Config;
using Elders.Cronus.Multitenancy;
using Elders.Cronus.AtomicAction;
using Elders.Cronus.Pipeline.Config;
using Elders.Cronus.Projections.Cassandra.Snapshots;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class CassandraProjectionStoreFactory : IProjectionStoreFactory
    {
        const string NO_TENANT_NAME = "notenant";

        readonly ITenantList _tenants;
        readonly CassandraProvider _cassandraProvider;
        readonly ISerializer _serializer;
        readonly IPublisher<ICommand> _publisher;
        private readonly ILock _lock;
        private readonly TypeContainer<IProjection> _projectionsTypes;
        readonly CassandraProjectionStoreStorageManager cassandraProjectionStoreStorageManager;
        private readonly CassandraSnapshotStoreSchema cassandraSnapshotSchema;
        IDictionary<string, IProjectionStore> _projectionStoresByTenant;

        public CassandraProjectionStoreFactory(ITenantList tenants, CassandraProvider cassandraProvider, ISerializer serializer, IPublisher<ICommand> publisher, ILock @lock, TypeContainer<IProjection> projectionsTypes, CassandraProjectionStoreStorageManager cassandraProjectionStoreStorageManager, CassandraSnapshotStoreSchema cassandraSnapshotSchema)
        {
            if (tenants is null) throw new ArgumentNullException(nameof(tenants));
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            if (serializer is null) throw new ArgumentNullException(nameof(serializer));
            if (publisher is null) throw new ArgumentNullException(nameof(publisher));
            if (@lock is null) throw new ArgumentNullException(nameof(@lock));
            if (_projectionsTypes is null) throw new ArgumentNullException(nameof(_projectionsTypes));

            _tenants = tenants;
            _cassandraProvider = cassandraProvider;
            _serializer = serializer;
            _publisher = publisher;
            this._lock = @lock;
            this._projectionsTypes = projectionsTypes;
            this.cassandraProjectionStoreStorageManager = cassandraProjectionStoreStorageManager;
            this.cassandraSnapshotSchema = cassandraSnapshotSchema;
            _projectionStoresByTenant = new Dictionary<string, IProjectionStore>();

            if (tenants.HasOtherTenantThanElders())
            {
                foreach (var tenant in tenants.GetTenants())
                {
                    InitializeTenantProjectionStoreInstance(tenant);
                }
            }
            else
            {
                //TODO MAYBE WE SHOULD ALSO INITIALIZE ELDERS TENANT?
                throw new Exception("MAYBE WE SHOULD ALSO INITIALIZE ELDERS TENANT?");
                InitializeTenantProjectionStoreInstance(NO_TENANT_NAME);
            }
        }

        public IProjectionStore GetProjectionStore(string tenant)
        {
            IProjectionStore projectionStore = null;

            _projectionStoresByTenant.TryGetValue(tenant, out projectionStore);

            if (ReferenceEquals(null, projectionStore))
                throw new Exception($"ProjectionStore for tenant {tenant} is not registered. Make sure that the tenant is registered in.");

            return projectionStore;
        }

        public IEnumerable<IProjectionStore> GetProjectionStores()
        {
            return _projectionStoresByTenant.Select(x => x.Value);
        }

        private void InitializeTenantProjectionStoreInstance(string tenant)
        {
            if (string.IsNullOrEmpty(tenant)) throw new ArgumentNullException(nameof(tenant));

            var projectionStore = new CassandraProjectionStore(_cassandraProvider, _serializer, _publisher, cassandraProjectionStoreStorageManager, cassandraSnapshotSchema);
            _projectionStoresByTenant.Add(tenant, projectionStore);
        }
    }
}
