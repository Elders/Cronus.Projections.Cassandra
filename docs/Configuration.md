# Cronus.Projections.Cassandra

## `Cronus:Projections:Cassandra:ConnectionString` >> *string | Required: Yes*

The connection to the Cassandra database server

---

## `Cronus:Projections:Cassandra:ReplicationStrategy` >> *string | Required: No | Default: simple*

Configures Cassandra replication strategy. This setting has effect only in the first run when creating the database.

Valid values:

* simple
* network_topology - when using this setting you need to specify `Cronus:Projections:Cassandra:ReplicationFactor` and `Cronus:Projections:Cassandra:Datacenters` as well

---

## `Cronus:Projections:Cassandra:ReplicationFactor` >> *integer | Required: No | Default: 1*

---

## `Cronus:Projections:Cassandra:Datacenters` >> *string[] | Required: No*

---

## `Cronus:Projections:Cassandra:TableRetention:DeleteOldProjectionTables` >> *boolean | Required: No | Default: false*

---

## `Cronus:Projections:Cassandra:TableRetention:NumberOfOldProjectionTablesToRetain` >> *unsigned integer | Required: No | Default: 2*
