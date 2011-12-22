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
particular library's specific functionality, perhpas one should not use
datastore and should directly use that library.

### Key

A Key represents the unique identifier of an object.

Our Key scheme is inspired by file systems and the Google App Engine key
model.

Keys are meant to be unique across a system. Keys are hierarchical,
incorporating more and more specific namespaces. Thus keys can be deemed
'children' or 'ancestors' of other keys.

    Key('/Comedy')
    Key('/Comedy/MontyPython')

Also, every namespace can be parametrized to embed relevant object
information. For example, the Key `name` (most specific namespace) could
include the object type:

    Key('/Comedy/MontyPython/Actor:JohnCleese')
    Key('/Comedy/MontyPython/Sketch:CheeseShop')
    Key('/Comedy/MontyPython/Sketch:CheeseShop/Character:Mousebender')


## Install

    git clone https://github.com/jbenet/datastore/
    cd datastore
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

