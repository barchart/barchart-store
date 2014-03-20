barchart-store
==============

Provides a simple, easy to use abstraction on top of different distributed key/value stores.
The motivation for these projects is to separate the connection setup and configuration from
the data API interactions, in a way that allows for easy unit testing of datastore-dependent
modules.

Current usage targets Cassandra for production use, although other key/value stores should
be fairly easy to implement.

#### Core modules:

store-api - The base API for store access

store-heap - A in-memory heap store implementation for use with local volatile data and unit testing

store-cassandra - A Cassandra store-api implementation which wraps Astyanax

#### Optional/utility modules:

store-util - Utility classes for data mapping, querying and version 1 UUID handling

store-model - Serializable model base classes for using with a store
