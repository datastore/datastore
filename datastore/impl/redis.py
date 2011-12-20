
__version__ = '1.0'
__author__ = 'Juan Batiz-Benet <jbenet@cs.stanford.edu>'
__doc__ = '''
redis datastore implementation.

Tested with:
redis 2.2.12
redis-py 2.4.10

'''

#TODO: Implements queries using a key index.

import datastore
import json




class NonSerializer(object):
  '''Implements serializing protocol but does not serialize at all.
  If only storing strings (or already-serialized values).
  '''
  @staticmethod
  def loads(value):
    return value

  @staticmethod
  def dumps(value):
    return value



def implements_redis_interface(client, interface=['set', 'get', 'delete']):
  '''Verifies that the client responds to the basic redis interface'''

  for method in interface:
    if not hasattr(client, method):
      raise ValueError('client %s does not implement %s' % (client, method))




class RedisDatastore(datastore.Datastore):
  '''Simple redis datastore. Does not support queries.'''

  # value serializer necessary because redis stores string values. clients can
  # override this with their own custom serializer on a class-wide or per-
  # instance basis. If you plan to store mostly strings, use NonSerializer
  serializer = json

  def __init__(self, client, serializer=None):
    '''Initialize the datastore with given redis `client`.

    Args:
      client: A redis client to use. Must implement the interface: set, get,
              delete (del is reserved in python). This datastore keeps the
              interface so basic in order to work with various libraries.

      serializer: An optional value serializer to use instead of the default
              (json). Serializer must respond to `loads` and `dumps`.
    '''
    implements_redis_interface(client)
    self.client = client

    if serializer:
      self.serializer = serializer

    # ensure serializer works
    test = repr(self)
    errstr = 'Serializer error: serialized value does not match original'
    assert self.serializer.loads(self.serializer.dumps(test)) == test, errstr


  def get(self, key):
    '''Return the object named by key or None if it does not exist.
    None takes the role of default value, so no KeyError exception is raised.

    Args:
      key: Key naming the object to retrieve

    Returns:
      object or None
    '''
    value = self.client.get(str(key))
    if value:
      value = self.serializer.loads(value)
    return value


  def put(self, key, value):
    '''Stores the object `value` named by `key`.
    How to serialize and store objects is up to the underlying datastore.
    It is recommended to use simple objects (strings, numbers, lists, dicts).

    Args:
      key: Key naming `value`
      value: the object to store.
    '''
    value = self.serializer.dumps(value)
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
    #TODO: remember to deserialize values.
    raise NotImplementedError


  def deserialize_gen(self, iterable):
    '''Generator that deserializes objects from `iterable`.'''
    for item in iterable:
      yield self.serializer.loads(item)
