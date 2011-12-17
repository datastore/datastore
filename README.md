# datastore

## Unified API for multiple data stores.

datastore is a generic layer of abstraction for data store and database access.
It is a **simple** APi with the aim to enable application development in a
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

Raturns:
  iterable with all objects matching criteria


### Specialized Features

Datastore implementors are free to implement specialized features, pertinent only to a single datastore, with the


## Key

Datastore uses a special Key object.


## Install


    wget https://github.com/jbenet/datastore/raw
    sudo python setup.py install

And soon:

    sudo pip install datastore

or

    sudo easy_install datastore

## License

datastore is under the MIT License.

## Hello World

    >>> import datastore
    >>> ds = datastore.basic.DictDatastore()
    >>> hello = datastore.Key('hello')
    >>> ds.put(hello, 'world')
    >>> ds.contains(hello)
    True
    >>> ds.get(hello)
    'world'
    >>> ds.delete(hello)
    >>> ds.get(hello)
    None

