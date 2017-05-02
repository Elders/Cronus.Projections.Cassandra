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
