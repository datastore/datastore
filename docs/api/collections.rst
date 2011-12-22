.. _api-collections:

Collections
===========

Grouping datastores into datastore collections can
significantly simplify complex access patterns. For example, caching datastores
can be checked before accessing more costly datastores, or a group of
equivalent datastores can act as shards containing large data sets.

As :ref:`shims <api-shims>`, datastore collections also derive from
:ref:`datastore <api-datastore>`, and must implement the four datastore
operations (get, put, delete, query).


DatastoreCollection
-------------------

Collections may derive from DatastoreCollection


.. autoclass:: datastore.DatastoreCollection
   :members:


TieredDatastore
---------------

.. autoclass:: datastore.TieredDatastore
   :members:


ShardedDatastore
----------------

.. autoclass:: datastore.ShardedDatastore
   :members:
