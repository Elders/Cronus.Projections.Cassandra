# [6.3.0-preview.1](https://github.com/Elders/Cronus.Projections.Cassandra/compare/v6.3.0-next.4...v6.3.0-preview.1) (2021-05-05)


### Bug Fixes

* Consolidates release notes ([c33dbb9](https://github.com/Elders/Cronus.Projections.Cassandra/commit/c33dbb95542636951d331966b8303e7b3356afe3))
* Switches to beta/preview branches ([024274f](https://github.com/Elders/Cronus.Projections.Cassandra/commit/024274fb339daa113a558390ef9772e93fb685cc))
* Updates packages ([90fbb41](https://github.com/Elders/Cronus.Projections.Cassandra/commit/90fbb41a767abd9691bb74a985f5ba3ae6935a41))

# [6.3.0-next.4](https://github.com/Elders/Cronus.Projections.Cassandra/compare/v6.3.0-next.3...v6.3.0-next.4) (2021-03-23)


### Bug Fixes

* Fix creation and usage of prepare statements when reading from projections ([29faf7e](https://github.com/Elders/Cronus.Projections.Cassandra/commit/29faf7e3b5ad6788a554b7de0b999946eaad4097))

# [6.3.0-next.3](https://github.com/Elders/Cronus.Projections.Cassandra/compare/v6.3.0-next.2...v6.3.0-next.3) (2021-03-18)


### Bug Fixes

* Sets consistency level to all queries to be local quorum ([8921b20](https://github.com/Elders/Cronus.Projections.Cassandra/commit/8921b200c2cbd082a0e829eea559eb8956e8c9bf))

# [6.3.0-next.2](https://github.com/Elders/Cronus.Projections.Cassandra/compare/v6.3.0-next.1...v6.3.0-next.2) (2021-03-17)


### Features

* Implement HasSnapshotMarkerAsync ([bbca182](https://github.com/Elders/Cronus.Projections.Cassandra/commit/bbca18295b5e95a23ee417d3547d595e3a9f147a))

# [6.3.0-next.1](https://github.com/Elders/Cronus.Projections.Cassandra/compare/v6.2.1-next.2...v6.3.0-next.1) (2021-03-16)


### Features

* Updates Cronus ([5df5423](https://github.com/Elders/Cronus.Projections.Cassandra/commit/5df5423c6225ce6aaf3f1e0073f64101fb75c9c3))

## [6.2.1-next.2](https://github.com/Elders/Cronus.Projections.Cassandra/compare/v6.2.1-next.1...v6.2.1-next.2) (2021-01-29)


### Bug Fixes

* Changes the projections default sorting in the database from ARID to event timestamp ([a723989](https://github.com/Elders/Cronus.Projections.Cassandra/commit/a7239891d5d7ae4f54ebf92a3b7eb88664729995))

## [6.2.1-next.1](https://github.com/Elders/Cronus.Projections.Cassandra/compare/v6.2.0...v6.2.1-next.1) (2021-01-25)


### Bug Fixes

* Switches to azure pipelines and semantic release ([673576f](https://github.com/Elders/Cronus.Projections.Cassandra/commit/673576f705c4d01332edd2d72d26152e3fd44643))

#### 6.2.0 - 01.10.2020
* Reworks projection naming

#### 6.1.1 - 24.09.2020
* Starts using GetSession from CassandraProvider instead of local copies for connections
* Updates Cronus packaget to 6.1.1

#### 6.1.0 - 24.08.2020
* Updates packages

#### 6.0.0 - 16.04.2020
* Rework the CassandraProvideroptions to use options pattern
* Reworks slightly the CassandraProvider
* Changes the target framework to netcoreapp3.1
* Allows consumers to configure the cassandra session

#### 5.3.1 - 02.05.2019
* Fixes a memory leak due to TCP Socket management

#### 5.3.0 - 05.02.2019
* CassandraProjectionStoreSchema is now used by Interface
* Updated Cronus to 5.3.0
* Removed Prepared statements from the Keyspace initializers as they are not supported by Azure Cassandra

#### 5.2.0 - 10.12.2018
* Adds options for the cassandra provider
* Adds async support for the projection snapshot
* Drops net472 because netstandard2.0 supports it out of the box

#### 5.1.0 - 10.12.2018
* Updates to DNC 2.2

#### 5.0.0 - 29.11.2018
* Removes obsolete throw; statements
* Fixes the IProjectionsNamingStrategy interface
* Adds CassandraSnapshotStore with open generics registration in the DI
* Various fixes and improvements
* Replaces extensions which were providing column family naming with IProjectionsNamingStrategy
* Reworks the CassandraProvider
* Fixes the CassandraProjectionStoreSchema dependencies
* Updates the projection discovery
* Adds IProjectionLoader discovery
* Implements Async functionalities for IProjectionStore
* Expose locking for creating and dropping tables
* Removed CanChangeSchema
* Create snapshot tables if the load query fails
* Create schema session with the already created keyspace
* Create table if projection query fails
* Improved logging when initializing projection store
* Improved logging when initializing projection snapshot store
* Getting live session for projection schema creation
* Only certain nodes can create and delete Cassandra tables and keyspaces
* It is ok to create a keyspace from multiple threads
* Create default keyspace before connecting to the cluster
* Create projection store schema against a single node
* Create snapshot store schema against a single node
* The version revisions are now part of the cassandra column family names
* Implements a method to return a SnapshotMeta information without the snapshot state
* TimeOffsetSnapshotStrategy is the default strategy for projection reads
* EventsCountSnapshotStrategy is the default strategy for projection writes
* Enabled SourceLink
* Removed UseSnapshot because it caused problems to snapshots if used. By default snapshots used

#### 3.0.3 - 28.02.2018
* Updates Cronus to 4.0.10

#### 3.0.2 - 26.02.2018
* Updates Cronus to 4.0.8

#### 3.0.1 - 20.02.2018
* Targets netstandard2.0;net45;net451;net452;net46;net461;net462

#### 3.0.0 - 13.02.2018
* netstandard2.0

#### 2.2.12 - 12.01.2018
* Changes snapshot creation to trigger only during read (load of projection) - sorry for breaking the public API and not bumping the version

#### 2.2.11 - 14.12.2017
* Adds ability to shoot yourself in the foot

#### 2.2.10 - 14.09.2017
* Move ProjectionCommit in Cronus

#### 2.2.9 - 07.09.2017
* Fixes snapshot creation bug

#### 2.2.8 - 07.09.2017
* Improves logging even more!!!

#### 2.2.7 - 07.09.2017
* Guess what?!?! Improves logging even more!!

#### 2.2.6 - 07.09.2017
* Improves logging even more!!

#### 2.2.5 - 07.09.2017
* Fixes a bug when loading a projection

#### 2.2.4 - 07.09.2017
* Improves logging even more!

#### 2.2.3 - 07.09.2017
* Improves logging even more

#### 2.2.2 - 07.09.2017
* Adds some debug logs

#### 2.2.1 - 06.09.2017
* Performance optimizations

#### 2.2.0 - 05.09.2017
* Changes the default configuration for snapshot `new DefaultSnapshotStrategy(snapshotOffset: TimeSpan.FromDays(1), eventsInSnapshot: 500)`
* Updates packages. There were small breaking changes for projections from DomainModeling

#### 2.1.1 - 18.07.2017
* Adds warning to prevent potential memory leak when snapshots are not enabled

#### 2.1.0 - 18.07.2017
* Fixes applying of snapshots
* Fixes backward compatibility issue
* Updates Cronus packages
* Move common interfaces to Cronus.DomainModeling
* Fixes issue with creating wrong table for snapshots
* Fixes issue with creating wrong table for snapshots
* Adds null checks
* Fixes Cassandra table creation issue
* EventSourcedProjectionBuilder now handles version state internally
* Adds ability to replay projections
* Fix to cassandra snapshot store
* Added snapshot strategy
* Added snapshot store

#### 2.0.1 - 26.04.2017
* Fixes database initialization
* Removes the old projections

#### 2.0.0 - 26.04.2017
* Added event sourced projections
* Added support for Cassandra cluster

#### 2.0.0-beta0014 - 26.04.2017
* Fix projections settings

#### 2.0.0-beta0013 - 25.04.2017
* Fix event sourced projections middleware registration

#### 2.0.0-beta0012 - 19.04.2017
* Move the event sourced projections middleware registration to the UseEventSourcedProjections under the CassandraProjectionsSettings

#### 2.0.0-beta0011 - 19.04.2017
* Add setting for using event sourced projections or normal projections

#### 2.0.0-beta0010 - 19.04.2017
* Change the configuration extensions names so that they do not collide with the event store configurations

#### 2.0.0-beta0009 - 18.04.2017
* Add settings for read and write consistency level

#### 2.0.0-beta0008 - 13.04.2017
* Changes the "SetConnectionString" to "SetProjectionsConnectionString"

#### 2.0.0-beta0007 - 13.04.2017
* Changes to the settings API

#### 2.0.0-beta0006 - 13.04.2017
* Change the settings API entry point

#### 2.0.0-beta0005 - 13.04.2017
* Add the ability to use the cassandra projections with easy to use settings API

#### 2.0.0-beta0004 - 20.02.2017
* Projection result is an interface now

#### 2.0.0-beta0003 - 01.12.2016
* Fix loading of projections.

#### 2.0.0-beta0002 - 30.11.2016
* Should work

#### 2.0.0-beta0001 - 24.11.2016
* Initial implementation with event sourced projections

#### 1.1.1 - 15.10.2016
* Fixed issue when loading collection item after loading collection
* Added implementation for delete by id
* Added public non generic delete by id
* Added public non generic delete of collection item by collection id and item id
* Support retrieving single collection item

#### 1.0.3 - 08.04.2016
* Additional methods for saving and retrieving

#### 1.0.2 - 06.04.2016
* Added public non generic get by id

#### 1.0.1 - 13.07.2015
* Use columnfamily everywhere

#### 1.0.0 - 06.07.2015
* Initial version
