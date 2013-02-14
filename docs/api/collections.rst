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


Example:

    >>> import pymongo
    >>> import datastore.core
    >>>
    >>> from datastore.mongo import MongoDatastore
    >>> from datastore.pylru import LRUCacheDatastore
    >>> from datastore.filesystem import FileSystemDatastore
    >>>
    >>> conn = pymongo.Connection()
    >>> mongo = MongoDatastore(conn.test_db)
    >>>
    >>> cache = LRUCacheDatastore(1000)
    >>> fs = FileSystemDatastore('/tmp/.test_db')
    >>>
    >>> ds = datastore.TieredDatastore([cache, mongo, fs])
    >>>
    >>> hello = datastore.Key('hello')
    >>> ds.put(hello, 'world')
    >>> ds.contains(hello)
    True
    >>> ds.get(hello)
    'world'
    >>> ds.delete(hello)
    >>> ds.get(hello)
    None


ShardedDatastore
----------------

.. autoclass:: datastore.ShardedDatastore
   :members:


Example:


    >>> import datastore.core
    >>>
    >>> shards = [datastore.DictDatastore() for i in range(0, 10)]
    >>>
    >>> ds = datastore.ShardedDatastore(shards)
    >>>
    >>> hello = datastore.Key('hello')
    >>> ds.put(hello, 'world')
    >>> ds.contains(hello)
    True
    >>> ds.get(hello)
    'world'
    >>> ds.delete(hello)
    >>> ds.get(hello)
    None
