using Elders.Cronus.DomainModeling;
using Elders.Cronus.Projections.Cassandra.EventSourcing;
using Elders.Cronus.Projections.Cassandra.Snapshots;
using Machine.Specifications;
using System;
using System.Collections.Generic;

namespace Elders.Cronus.Projections.Cassandra.Tests
{
    public class When_loadng_projection_from_stream
    {
        static ProjectionStream stream;
        static List<ProjectionCommit> commits;
        static Id id;
        static int numberOfInstances;

        Establish context = () =>
        {
            numberOfInstances = 0;
            id = new Id(Guid.NewGuid().ToString());
            commits = new List<ProjectionCommit>();

            for (int i = 1; i < 10; i++)
            {
                var @event = new Event() { Id = id };
                commits.Add(new ProjectionCommit(id, "Projection", @event, i, new EventOrigin(id.Urn.Value, i, 1, DateTime.UtcNow.ToFileTimeUtc()), DateTime.UtcNow));
            }

            for (int i = 1; i < 10; i++)
            {
                var @event = new Event1() { Id = id };
                commits.Add(new ProjectionCommit(id, "Projection", @event, i, new EventOrigin(id.Urn.Value, i, 1, DateTime.UtcNow.ToFileTimeUtc()), DateTime.UtcNow));
            }

            stream = new ProjectionStream(commits, new NoSnapshot(id, "Projection"));
        };

        Because of = () =>
        {
            stream.RestoreFromHistory<Projection>();
            numberOfInstances = Projection.instances;
        };

        It should_create_one_instance_of_the_projection = () =>
        {
            numberOfInstances.ShouldEqual(1);
        };

        public class Projection : ProjectionDefinition<ProjectionState, Id>,
            IEventHandler<Event>,
            IEventHandler<Event1>
        {
            public static int instances;
            public Projection()
            {
                Subscribe<Event>(x => x.Id);
                Subscribe<Event1>(x => x.Id);
                instances++;
            }

            public void Handle(Event @event)
            {
            }

            public void Handle(Event1 @event)
            {
            }
        }

        public class ProjectionState
        {
            public Id Id { get; set; }
        }

        public class Event : IEvent
        {
            public Id Id { get; set; }
        }

        public class Event1 : IEvent
        {
            public Id Id { get; set; }
        }

        public class Id : StringTenantId
        {
            public Id(string id) : base(id, "id", "test") { }
        }
    }
}
