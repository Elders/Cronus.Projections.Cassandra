﻿using Elders.Cronus.Projections.Cassandra.Snapshots;
using System.Collections.Generic;
using System.Linq;
using System;
using Elders.Cronus.DomainModeling.Projections;
using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Projections.Cassandra.EventSourcing
{
    public class ProjectionStream
    {
        private readonly IBlobId projectionId;
        IList<ProjectionCommit> commits;
        readonly ISnapshot snapshot;

        public ProjectionStream(IBlobId projectionId, IList<ProjectionCommit> commits, ISnapshot snapshot)
        {
            if (ReferenceEquals(null, projectionId) == true) throw new ArgumentException(nameof(projectionId));
            if (ReferenceEquals(null, commits) == true) throw new ArgumentException(nameof(commits));
            if (ReferenceEquals(null, snapshot) == true) throw new ArgumentException(nameof(snapshot));

            this.projectionId = projectionId;
            this.commits = commits;
            this.snapshot = snapshot;
        }

        public IEnumerable<ProjectionCommit> Commits { get { return commits.ToList().AsReadOnly(); } }

        public IProjectionGetResult<IProjectionDefinition> RestoreFromHistory(Type projectionType)
        {
            if (commits.Count < 0) return ProjectionGetResult<IProjectionDefinition>.NoResult;

            IProjectionDefinition projection = (IProjectionDefinition)FastActivator.CreateInstance(projectionType, true);
            return RestoreFromHistoryMamamia(projection);
        }

        public IProjectionGetResult<T> RestoreFromHistory<T>() where T : IProjectionDefinition
        {
            if (commits.Count < 0) return ProjectionGetResult<T>.NoResult;

            T projection = (T)Activator.CreateInstance(typeof(T), true);
            return RestoreFromHistoryMamamia<T>(projection);
        }

        IProjectionGetResult<T> RestoreFromHistoryMamamia<T>(T projection) where T : IProjectionDefinition
        {
            projection.InitializeState(projectionId, snapshot.State);

            var groupedBySnapshotMarker = commits.GroupBy(x => x.SnapshotMarker).OrderBy(x => x.Key);
            foreach (var snapshotGroup in groupedBySnapshotMarker)
            {
                var eventsByAggregate = snapshotGroup.GroupBy(x => x.EventOrigin.AggregateRootId);

                foreach (var aggregateGroup in eventsByAggregate)
                {
                    var events = aggregateGroup
                        .OrderBy(x => x.EventOrigin.AggregateRevision)
                        .ThenBy(x => x.EventOrigin.AggregateEventPosition)
                        .Select(x => x.Event);

                    projection.ReplayEvents(events);
                }
            }

            return new ProjectionGetResult<T>(true, projection);
        }
    }
}
