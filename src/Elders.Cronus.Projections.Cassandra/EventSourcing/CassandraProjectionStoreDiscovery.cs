//using Elders.Cronus.Discoveries;
//using Elders.Cronus.Pipeline.Config;
//using System.Collections.Generic;

//namespace Elders.Cronus.Projections.Cassandra.EventSourcing
//{
//    public class CassandraProjectionStoreDiscovery : DiscoveryBasedOnExecutingDirAssemblies<IProjectionStore>
//    {
//        protected override DiscoveryResult<IProjectionStore> DiscoverFromAssemblies(DiscoveryContext context)
//        {
//            var result = new DiscoveryResult<IProjectionStore>(GetModels(context));


//            return result;
//        }

//        private IEnumerable<DiscoveredModel> GetModels(DiscoveryContext context)
//        {
//            yield return new DiscoveredModel(typeof(IProjectionStoreFactory), typeof(CassandraProjectionStoreFactory), Microsoft.Extensions.DependencyInjection.ServiceLifetime.Transient);

//            HandlerTypeContainer<IProjection> projectionHandlersContainer = new HandlerTypeContainer<IProjection>(new List<System.Type>());
//            yield return new DiscoveredModel(typeof(HandlerTypeContainer<IProjection>), projectionHandlersContainer);
//        }
//    }
//}
