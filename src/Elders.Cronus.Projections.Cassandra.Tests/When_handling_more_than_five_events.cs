using System;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.Projections.Cassandra.EventSourcing;
using Machine.Specifications;
using System.Collections.Generic;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Projections.Cassandra.Snapshots;
using Elders.Cronus.DomainModeling.Projections;

namespace Elders.Cronus.Projections.Cassandra.Tests
{
    public class When_handling_more_than_five_events
    {
        static EventSourcedProjectionsMiddleware middleware;
        static ISnapshotStore snapshotStore;
        static IProjectionStore projectionStore;
        static ISnapshotStrategy snapshotStrategy;
        static TestEvent @event;
        static Id testId;
        static Dictionary<string, string> headers;

        Establish context = () =>
        {
            projectionStore = new MemoryProjectionStore();
            snapshotStore = new MemorySnapshotStore();
            snapshotStrategy = new DefaultSnapshotStrategy(TimeSpan.Zero, 5);

            middleware = new EventSourcedProjectionsMiddleware(projectionStore, snapshotStore, snapshotStrategy);
            testId = new Id("test");
            @event = new TestEvent() { Id = testId };
            headers = new Dictionary<string, string>()
            {
                {MessageHeader.AggregateRootId, "test" },
                {MessageHeader.AggregateRootRevision, "1" }
            };

            for (var i = 1; i <= 10; i++)
            {
                middleware.Run(new HandleContext(
                    new CronusMessage(@event, headers),
                    typeof(TestProjection)));
            }
        };

        Because of = () =>
        {
            middleware.Run(new HandleContext(
                new CronusMessage(@event, headers),
                typeof(TestProjection)));
        };

        It should_create_a_snpshot = () =>
        {
            snapshotStore.Load("TestProjection", testId).Revision.ShouldEqual(2);
        };

        It should_build_correct_state = () =>
        {
            (snapshotStore.Load("TestProjection", testId).State as ProjectionState).Counter.ShouldEqual(10);
        };

        public class TestProjection : ProjectionDefinition<ProjectionState, Id>,
            IEventHandler<TestEvent>
        {
            public TestProjection()
            {
                Subscribe<TestEvent>(x => x.Id);
            }

            public void Handle(TestEvent @event)
            {
                State.Counter++;
            }

        }

        [Serializable]
        public class ProjectionState
        {
            public int Counter { get; set; }
        }

        public class TestEvent : IEvent
        {
            public Id Id { get; set; }
        }

        public class Id : StringTenantId
        {
            public Id(string id) : base(id, "id", "test") { }
        }
    }
}
