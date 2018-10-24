#### 5.0.0-discovery0004 - 24.10.2018
* Uses ICassandra provider in the ctors
* Code cleanup
* Trigger

#### 5.0.0-discovery0003 - 21.10.2018
* Updates the projection discovery

#### 5.0.0-discovery0002 - 08.10.2018
* Updates Cronus

#### 5.0.0-discovery0001 - 08.10.2018
* Adds IProjectionLoader discovery

#### 5.0.0-beta0016 - 02.10.2018
* Updates Cronus
* Implements Async functionalities for IProjectionStore

#### 5.0.0-beta0015 - 25.07.2018
* Expose locking for creating and dropping tables
* Removed CanChangeSchema

#### 5.0.0-beta0014 - 23.07.2018
* Create snapshot tables if the load query fails
* Create schema session with the already created keyspace
* Create table if projection query fails
* Improved logging when initializing projection store
* Improved logging when initializing projection snapshot store

#### 5.0.0-beta0013 - 23.07.2018
* Getting live session for projection schema creation
* Only certain nodes can create and delete Cassandra tables and keyspaces
* It is ok to create a keyspace from multiple threads

#### 5.0.0-beta0012 - 18.07.2018
* Do not set Cassandra port

#### 5.0.0-beta0011 - 17.07.2018
* Create default keyspace before connecting to the cluster

#### 5.0.0-beta0010 - 16.07.2018
* Create projection store schema against a single node
* Create snapshot store schema against a single node
* Updates Cronus to 5.0.0-beta0029

#### 5.0.0-beta0009 - 12.07.2018
* Using Cronus 5.0.0-beta0026

#### 5.0.0-beta0008 - 11.07.2018
* Using Cronus 5.0.0-beta0022
* The version revisions are now part of the cassandra column family names

#### 5.0.0-beta0007 - 10.07.2018
* Using Cronus 5.0.0-beta0021

#### 5.0.0-beta0006 - 18.06.2018
* Implements a method to return a SnapshotMeta information without the snapshot state
* TimeOffsetSnapshotStrategy is the default strategy for projection reads
* EventsCountSnapshotStrategy is the default strategy for projection writes

#### 5.0.0-beta0005 - 07.06.2018
* Enabled SourceLink

#### 5.0.0-beta0004 - 07.06.2018
* Removed UseSnapshot because it caused problems to snapshots if used. By default snapshots used 

#### 5.0.0-beta0003 - 02.04.2018
* Updates Cronus

#### 5.0.0-beta0002 - 02.04.2018
* Updates Cronus

#### 5.0.0-beta0001 - 01.04.2018
* Gets in line with Cronus v5

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
