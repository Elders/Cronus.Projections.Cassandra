using Elders.Cronus.DomainModeling;
using System;
using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.DomainModeling.Projections;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class ProjectionDefinition<TState, TId> : IProjectionDefinition, IProjection
        where TState : new()
        where TId : IBlobId
    {
        public ProjectionDefinition()
        {
            ((IProjectionDefinition)this).State = new TState();
            idsResolve = new Dictionary<Type, Func<IEvent, TId>>();
        }

        public TState State { get { return (TState)((IHaveState)this).State; } private set { ((IHaveState)this).State = value; } }

        object IHaveState.State { get; set; }

        Dictionary<Type, Func<IEvent, TId>> idsResolve;

        IBlobId IProjectionDefinition.GetProjectionId(IEvent @event)
        {
            return idsResolve[@event.GetType()](@event);
        }

        void IProjectionDefinition.Apply(IEvent @event)
        {
            ((dynamic)this).Handle((dynamic)@event);
        }

        void IAmEventSourcedProjection.ReplayEvents(IEnumerable<IEvent> events)
        {
            var def = this as IProjectionDefinition;
            events.ToList().ForEach(e => def.Apply(e));
        }

        void IHaveState.InitializeState(object state)
        {
            ((IHaveState)this).State = state ?? new TState();
        }

        protected ProjectionDefinition<TState, TId> Subscribe<TEvent>(Func<TEvent, TId> projectionId) where TEvent : IEvent
        {
            idsResolve.Add(typeof(TEvent), x => projectionId((TEvent)x));
            return this;
        }
    }
}
