using Elders.Cronus.Projections.Cassandra.Infrastructure;
using Microsoft.Extensions.Options;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Elders.Cronus.Projections.Cassandra
{
    public class CassandraMigrationStore : IProjectionStore
    {
        private readonly IProjectionStoreLegacy _legacyStore;
        private readonly IProjectionStoreNew _newStore;
        private readonly CassandraProviderOptions _options;

        public CassandraMigrationStore(IProjectionStoreLegacy legacyStore, IProjectionStoreNew newStore, IOptionsMonitor<CassandraProviderOptions> provider)
        {
            _legacyStore = legacyStore;
            _newStore = newStore;
            _options = provider.CurrentValue;
        }

        public Task EnumerateProjectionsAsync(ProjectionsOperator @operator, ProjectionQueryOptions options)
        {
            if (_options.LoadFromNewProjectionsTables)
                return _newStore.EnumerateProjectionsAsync(@operator, options);
            else
                return _legacyStore.EnumerateProjectionsAsync(@operator, options);
        }

        public IAsyncEnumerable<ProjectionCommit> LoadAsync(ProjectionVersion version, IBlobId projectionId)
        {
            throw new System.NotImplementedException();
        }

        public Task SaveAsync(ProjectionCommit commit)
        {
            return _legacyStore.SaveAsync(commit); // Always save in both tables until we remove the legacy projections altogether
        }
    }
}
