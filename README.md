# datastore

## simple, unified API for multiple data stores

datastore is a generic layer of abstraction for data store and database access.
It is a **simple** API with the aim to enable application development in a
datastore-agnostic way, allowing datastores to be swapped seamlessly without
changing application code. Thus, one can leverage different datastores with
different strengths without committing the application to one datastore
throughout its lifetime. It looks like this:

    +---------------+
    |  application  |    <--- No cumbersome SQL or Mongo specific queries!
    +---------------+
            |            <--- simple datastore API calls
    +---------------+
    |   datastore   |    <--- datastore implementation for underlying db
    +---------------+
            |            <--- database specific calls
    +---------------+
    |  various dbs  |    <--- MySQL, Redis, MongoDB, FS, ...
    +---------------+

In addition, grouped datastores significantly simplify interesting data access
patterns (such as caching and sharding).



## About

### Install

From pypi (using pip):

    sudo pip install datastore

From pypi (using setuptools):

    sudo easy_install datastore

From source:

    git clone https://github.com/jbenet/datastore/
    cd datastore
    sudo python setup.py install


### Documentation

The documentation can be found at:
http://datastore.readthedocs.org/en/latest/

### License

datastore is under the MIT License.

### Contact

datastore is written by [Juan Batiz-Benet](https://github.com/jbenet). It
was originally part of [py-dronestore](https://github.com/jbenet/py-dronestore).
On December 2011, it was re-written as a standalone project.

Project Homepage:
[https://github.com/jbenet/datastore](https://github.com/jbenet/datastore)

Feel free to contact me. But please file issues in github first. Cheers!

## Contributing

### Implementations

Please write and contribute implementations for other data stores. This project
can only be complete with lots of help.

### Style

Please follow proper pythonic style in your code.

See [PEP 8](http://www.python.org/dev/peps/pep-0008/) and the [Google Python
Style Guide](http://google-styleguide.googlecode.com/svn/trunk/pyguide.html).

### Docs

Please document all code. ``datastore`` uses ``sphinx`` for documentation. Take
a look at the ``docs/`` directory.

To make sure the documentation compiles, run:

  cd docs
  make html
  open .build/html/index.html

Which should -- if all goes well -- open your favorite browser on the
newly-built docs.

## Examples

### Hello World

    >>> import datastore
    >>> ds = datastore.basic.DictDatastore()
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

#### Hello memcache

    >>> import pylibmc
    >>> import datastore
    >>> from datastore.impl.memcached import MemcachedDatastore
    >>> mc = pylibmc.Client(['127.0.0.1'])
    >>> ds = MemcachedDatastore(mc)
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

#### Hello mongo

    >>> import pymongo
    >>> import datastore
    >>> from datastore.impl.mongo import MongoDatastore
    >>>
    >>> conn = pymongo.Connection()
    >>> ds = MongoDatastore(conn.test_db)
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

#### Hello redis

    >>> import redis
    >>> import datastore
    >>> from datastore.impl.redis import RedisDatastore
    >>> r = redis.Redis()
    >>> ds = RedisDatastore(r)
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
#### Hello filesystem

    >>> import datastore
    >>> from datastore.impl.filesystem import FileSystemDatastore
    >>>
    >>> ds = FileSystemDatastore('/tmp/.test_datastore')
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

#### Hello git

    >>> import datastore
    >>> from datastore.impl.git import GitDatastore
    >>>
    >>> ds = GitDatastore('/tmp/.test_datastore')
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


#### Hello Tiered Access


    >>> import pymongo
    >>> import datastore
    >>>
    >>> from datastore.impl.mongo import MongoDatastore
    >>> from datastore.impl.lrucache import LRUCache
    >>> from datastore.impl.filesystem import FileSystemDatastore
    >>>
    >>> conn = pymongo.Connection()
    >>> mongo = MongoDatastore(conn.test_db)
    >>>
    >>> cache = LRUCache(1000)
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


#### Hello Sharding

    >>> import datastore
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


## API

The datastore API places an emphasis on  **simplicity** and elegance. Only four
core methods must be implemented (get, put, delete, query).

### get(key)

Return the object named by key or None if it does not exist.

    Args:
      key: Key naming the object to retrieve

    Returns:
      object or None

### put(key, value)

Stores the object `value` named by `key`.
How to serialize and store objects is up to the underlying datastore.
It is recommended to use simple objects (strings, numbers, lists, dicts).

    Args:
      key: Key naming `value`
      value: the object to store.

### delete(key)

Removes the object named by `key`.

    Args:
      key: Key naming the object to remove.

### query(query):

Returns an iterable of objects matching criteria expressed in `query`
Implementations of query will be the largest differentiating factor
amongst datastores. All datastores **must** implement query, even using
query's worst case scenario, see Query class for details.

    Args:
      query: Query object describing the objects to return.

    Returns:
      iterable cursor with all objects matching criteria


### Specialized Features

Datastore implementors are free to implement specialized features, pertinent
only to a subset of datastores, with the understanding that these should aim
for generality and will most likely not be implemented across other datastores.

When implementings such features, please remember the goal of this project:
simple, unified API for multiple data stores. When making heavy use of a
particular library's specific functionality, perhaps one should not use
datastore and should directly use that library.

### Key

A Key represents the unique identifier of an object.

Our Key scheme is inspired by file systems and the Google App Engine key
model.

Keys are meant to be unique across a system. Keys are hierarchical,
incorporating increasingly specific namespaces. Thus keys can be deemed
'children' or 'ancestors' of other keys.

    Key('/Comedy')
    Key('/Comedy/MontyPython')

Also, every namespace can be parametrized to embed relevant object
information. For example, the Key `name` (most specific namespace) could
include the object type:

    Key('/Comedy/MontyPython/Actor:JohnCleese')
    Key('/Comedy/MontyPython/Sketch:CheeseShop')
    Key('/Comedy/MontyPython/Sketch:CheeseShop/Character:Mousebender')

