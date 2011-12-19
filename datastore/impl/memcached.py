
__version__ = '1.0'
__author__ = 'Juan Batiz-Benet <jbenet@cs.stanford.edu>'
__doc__ = '''
memcached datastore implementation.

Tested with:
memcached 1.4.5
libmemcached 0.50
pylibmc 1.2.2

'''

import datastore


def responds_to_memcached_interface(client, interface=['set', 'get', 'delete']):
  '''Verifies that the client responds to the basic memcached interface'''

  for method in interface:
    if not hasattr(client, method):
      raise ValueError('client %s does not implement %s' % (client, method))



class MemcachedDatastore(datastore.Datastore):
  '''Flat memcached datastore. Does not support queries.'''

  def __init__(self, client):
    '''Initialize the datastore with given memcached `client`.

    Args:
      client: A memcached client to use. Must implement the basic memcached
              interface: set, get, delete. This datastore keeps the interface
              so basic in order to work with any memcached client (or pool of
              clients).
    '''
    responds_to_memcached_interface(client)
    self.client = client


  def get(self, key):
    '''Return the object named by key or None if it does not exist.
    None takes the role of default value, so no KeyError exception is raised.

    Args:
      key: Key naming the object to retrieve

    Returns:
      object or None
    '''
    return self.client.get(str(key))

  def put(self, key, value):
    '''Stores the object `value` named by `key`.
    How to serialize and store objects is up to the underlying datastore.
    It is recommended to use simple objects (strings, numbers, lists, dicts).

    Args:
      key: Key naming `value`
      value: the object to store.
    '''
    self.client.set(str(key), value)

  def delete(self, key):
    '''Removes the object named by `key`.

    Args:
      key: Key naming the object to remove.
    '''
    self.client.delete(str(key))

  def query(self, query):
    '''Returns an iterable of objects matching criteria expressed in `query`
    Implementations of query will be the largest differentiating factor
    amongst datastores. All datastores **must** implement query, even using
    query's worst case scenario, see Query class for details.

    Args:
      query: Query object describing the objects to return.

    Raturns:
      Cursor with all objects matching criteria
    '''
    raise NotImplementedError
