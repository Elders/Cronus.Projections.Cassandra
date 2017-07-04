using Elders.Cronus.DomainModeling;
using System.Runtime.Serialization;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class UserCreated : IEvent
    {
        public StringTenantId UserId { get; set; }
    }

    ///New Projections
    public class UserState
    {
        public StringTenantId Id { get; private set; }

        public string SomethingAwsome { get; set; }

    }

    #region Immutable projections
    [DataContract(Name = "94e7b8ac-1730-4e4a-ab27-09ddff152a26")]
    public class UserProjection : ProjectionDefinition<UserState, StringTenantId>,
           IEventHandler<UserCreated>
    {
        public UserProjection()
        {
            base.Subscribe<UserCreated>(x => x.UserId);
        }

        public void Handle(UserCreated @event)
        {
            State.SomethingAwsome = "";
        }
    }

    [DataContract(Name = "077c0f28-7b80-4930-8fc0-392e9442824c")]
    public class UserProjectionInAnotherNameSPace : ProjectionDefinition<UserState, StringTenantId>,
           IEventHandler<UserCreated>
    {
        public UserProjectionInAnotherNameSPace()
        {
            base.Subscribe<UserCreated>(x => x.UserId);
        }

        public void Handle(UserCreated @event)
        {
            State.SomethingAwsome = "";
            State.SomethingAwsome = "";
        }
    }
    #endregion


    public class UI
    {
        public UI()
        {
            IProjectionRepository repo = null;
            var proj = repo.Get<UserProjection>(null).Projection.State;
        }
    }
}
